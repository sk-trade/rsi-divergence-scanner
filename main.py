import uuid
import time
import pandas as pd
import ta
import threading
from typing import Dict, List, Callable, Optional
from collections import deque

# 모듈화된 파일에서 필요한 클래스와 함수 임포트
import config
from common.logger_setup import setup_logger, shutdown_logger, send_direct_webhook
from common.zmq_client import ZMQClient
from common.ranking_manager import RankingManager

# --- 전역 변수 및 로거 설정 ---
CLIENT_ID = f"chart-analyzer-{uuid.uuid4().hex[:4]}"
logger = setup_logger()


class ChartAnalyzer:
    """
    차트 데이터를 분석하고 신호를 생성하는 클래스.
    """
    def __init__(self, ranking_manager: RankingManager):
        self.ranking_manager = ranking_manager
        
        # 상태를 클래스 내부에 캡슐화
        self.divergence_tracker = {
            'low': {'last_point': None},
            'high': {'last_point': None}
        }
        
    def _calculate_vpoc(self, df: pd.DataFrame, bin_size_multiplier: float = 0.005) -> Optional[float]:
        """
        캔들의 고가/저가 범위를 모두 고려하여 더 정확한 VPOC(거래량 최다 집중 가격대)를 계산
        """
        if df.empty or len(df) < 2:
            return None

        last_close = df['close'].iloc[-1]
        bin_size = last_close * bin_size_multiplier
        if bin_size <= 0:
            return None

        volume_profile = {}
        for _, row in df.iterrows():
            low_bin = int(row['low'] / bin_size)
            high_bin = int(row['high'] / bin_size)
            num_bins = high_bin - low_bin + 1
            volume_per_bin = row['volume'] / num_bins if num_bins > 0 else 0
            for bin_index in range(low_bin, high_bin + 1):
                price_level = bin_index * bin_size
                volume_profile[price_level] = volume_profile.get(price_level, 0) + volume_per_bin
        
        if not volume_profile:
            return None
        
        return max(volume_profile, key=volume_profile.get)


    def analyze(self, candle_deques: Dict[str, deque]):
        """
        캔들 데이터를 분석하여 시장 구조 및 추세를 파악하고,
        그 컨텍스트 안에서 저점/고점 신호를 평가하여 RankingManager에게 명령합니다.
        """
        try:
            base_candle_deque = candle_deques[config.BASE_INTERVAL]
        except KeyError:
            return

        # --- 데이터 준비 ---
        if len(base_candle_deque) < config.LONG_TERM_MA_WINDOW + 1:
            return
        
        df = pd.DataFrame(list(base_candle_deque))
        try:
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['volume'] = df['volume'].astype(float)
        except (KeyError, TypeError) as e:
            logger.error(f"데이터프레임 처리 중 필수 컬럼 오류: {e}")
            return

        # 지표 계산
        df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=config.RSI_WINDOW).rsi()
        df['ma_long'] = ta.trend.sma_indicator(df['close'], window=config.LONG_TERM_MA_WINDOW)
        
        latest = df.iloc[-1]
        if pd.isna(latest['rsi']) or pd.isna(latest['ma_long']):
            return

        # 시장 컨텍스트 정의
        current_trend = "상승" if latest['close'] > latest['ma_long'] else "하락"

        # VPOC 계산 (개선된 로직은 유지)
        recent_data_for_vpoc = df.tail(config.MARKET_STRUCTURE_CANDLE_COUNT)
        most_traded_level = self._calculate_vpoc(recent_data_for_vpoc)

        # 상태 메시지 및 데이터 준비
        status_msg = (f"\r[{config.SYMBOL}:{config.BASE_INTERVAL}] "
                      f"Close: {latest['close']:<10,.0f} | "
                      f"RSI: {latest['rsi']:.2f} | "
                      f"추세: {current_trend} | "
                      f"탐색(L/H): {'ON' if self.ranking_manager.is_seeking('low') else 'OFF'}/{'ON' if self.ranking_manager.is_seeking('high') else 'OFF'}{' '*5}")
        print(status_msg, end="")

        latest_price = latest['close']
        latest_timestamp = latest['timestamp']
        indicator_data = { 'rsi': latest['rsi'] }

        # --- 전략적 의사결정  ---
        
        # 저점 탐색 전략
        if latest['rsi'] <= config.RSI_LOW_THRESHOLD and not self.ranking_manager.is_seeking('low'):
            context_info = f" ({current_trend} 추세 중)"
            self.ranking_manager.start_seeking('low', latest_price, latest_timestamp, indicator_data, context_info)
            self.divergence_tracker['low']['last_point'] = None

        # 탐색 진행 조건: 탐색 중 & 아직 과매도 구간을 벗어나지 않았을 때
        elif self.ranking_manager.is_seeking('low') and latest['rsi'] < config.RSI_EXIT_THRESHOLD:
            # 더 낮은 저점이 나오면 갱신
            current_low_session = self.ranking_manager.get_current_session_info('low')
            if current_low_session and latest_price < current_low_session['price']:
                # 다이버전스 체크 
                last_low_point = self.divergence_tracker['low']['last_point']
                if last_low_point and latest_price < last_low_point['price'] and indicator_data['rsi'] > last_low_point['indicators']['rsi']:
                    div_msg = (f"🔥🔥🔥 강세 다이버전스 출현 가능성! 🔥🔥🔥\n"
                               f" - 가격: {last_low_point['price']:,.0f}원 -> {latest_price:,.0f}원 (하락)\n"
                               f" - RSI: {last_low_point['indicators']['rsi']:.2f} -> {indicator_data['rsi']:.2f} (상승)")
                    logger.warning(div_msg)
                    send_direct_webhook(div_msg)
                self.divergence_tracker['low']['last_point'] = {'price': latest_price, 'indicators': indicator_data, 'timestamp': latest_timestamp}
            
            # 가격이 더 낮아졌을 때만 갱신하도록 RankingManager에 위임
            self.ranking_manager.update_if_needed('low', latest_price, latest_timestamp, indicator_data)

        # 탐색 종료 조건: 탐색 중 & 과매도 구간을 벗어났을 때
        elif self.ranking_manager.is_seeking('low') and latest['rsi'] >= config.RSI_EXIT_THRESHOLD:
            final_low_session = self.ranking_manager.get_current_session_info('low')
            if final_low_session:
                rebound_percent = ((latest_price - final_low_session['price']) / final_low_session['price']) * 100
                context_info = ""
                if most_traded_level and abs(final_low_session['price'] - most_traded_level) / most_traded_level < (config.KEY_LEVEL_TOLERANCE_PERCENT / 100):
                    context_info = " (🔥핵심 지지 구간에서 반등!)"
                self.ranking_manager.end_seeking('low', rebound_percent, context_info)
            else:
                self.ranking_manager.end_seeking('low', 0.0)

        # --- 고점 탐색 전략 ---
        # 탐색 시작 조건: 과매수 진입 & 탐색 중이 아닐 때
        if latest['rsi'] >= config.RSI_HIGH_THRESHOLD and not self.ranking_manager.is_seeking('high'):
            context_info = f" ({current_trend} 추세 중)"
            self.ranking_manager.start_seeking('high', latest_price, latest_timestamp, indicator_data, context_info)
            self.divergence_tracker['high']['last_point'] = None
        
        # 탐색 진행 조건: 탐색 중 & 아직 과매수 구간을 벗어나지 않았을 때
        elif self.ranking_manager.is_seeking('high') and latest['rsi'] > config.RSI_HIGH_EXIT_THRESHOLD:
            # 더 높은 고점이 나오면 갱신
            current_high_session = self.ranking_manager.get_current_session_info('high')
            if current_high_session and latest_price > current_high_session['price']:
                # 다이버전스 체크
                last_high_point = self.divergence_tracker['high']['last_point']
                if last_high_point and latest_price > last_high_point['price'] and indicator_data['rsi'] < last_high_point['indicators']['rsi']:
                    div_msg = (f"📉📉📉 약세 다이버전스 출현 가능성! 📉📉📉\n"
                            f" - 가격: {last_high_point['price']:,.0f}원 -> {latest_price:,.0f}원 (상승)\n"
                            f" - RSI: {last_high_point['indicators']['rsi']:.2f} -> {indicator_data['rsi']:.2f} (하락)")
                    logger.warning(div_msg)
                    send_direct_webhook(div_msg)
                self.divergence_tracker['high']['last_point'] = {'price': latest_price, 'indicators': indicator_data, 'timestamp': latest_timestamp}
            
            # 가격이 더 높아졌을 때만 갱신하도록 RankingManager에 위임
            self.ranking_manager.update_if_needed('high', latest_price, latest_timestamp, indicator_data)

        # 탐색 종료 조건: 탐색 중 & 과매수 구간을 벗어났을 때
        elif self.ranking_manager.is_seeking('high') and latest['rsi'] <= config.RSI_HIGH_EXIT_THRESHOLD:
            final_high_session = self.ranking_manager.get_current_session_info('high')
            if final_high_session:
                pullback_percent = ((final_high_session['price'] - final_high_session['price']) / final_high_session['price']) * 100
                context_info = ""
                if most_traded_level and abs(final_high_session['price'] - most_traded_level) / most_traded_level < (config.KEY_LEVEL_TOLERANCE_PERCENT / 100):
                    context_info = " (⚠️핵심 저항 구간에서 하락!)"
                self.ranking_manager.end_seeking('high', pullback_percent, context_info)
            else:
                self.ranking_manager.end_seeking('high', 0.0)


if __name__ == "__main__":
    start_msg = f"--- {CLIENT_ID}: '차트상 최저점' 분석기 시작 ---"
    logger.info(start_msg)
    
    ranking_manager = RankingManager()
    chart_analyzer = ChartAnalyzer(ranking_manager)
    
    client = ZMQClient(
        client_id=CLIENT_ID,
        symbol=config.SYMBOL,
        intervals=config.INTERVALS,
        candle_handler_callback=chart_analyzer.analyze
    )
    
    if client.start():
        ranking_manager.start_manager_thread()

        logger.info("--- 실시간 분석 중... (종료하려면 Ctrl+C) ---")
        try:
            while True:
                if not all(t.is_alive() for t in client.threads):
                    logger.error("ZMQ 백그라운드 스레드 중 하나가 예기치 않게 종료되었습니다.")
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(f"\n--- {CLIENT_ID}: 종료 신호 수신... ---")
        finally:
            logger.info("종료 절차를 시작합니다...")
            client.stop()
            ranking_manager.stop_manager_thread()
            logger.info("최종 랭킹 데이터를 파일에 저장합니다.")
            ranking_manager._save_boards()
            logger.warning(ranking_manager.get_final_ranking_board_message())
            shutdown_logger()
            print(f"\n--- {CLIENT_ID}: 모든 작업 완료 ---")