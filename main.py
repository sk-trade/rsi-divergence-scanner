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
        self.df = pd.DataFrame()
        
        self.divergence_tracker = {
            'low': {'last_point': None},
            'high': {'last_point': None}
        }
        
        self.max_data_length = config.LONG_TERM_MA_WINDOW + config.MARKET_STRUCTURE_CANDLE_COUNT + 150

      
    def _update_dataframe(self, candle_deque: deque):
        """
        새로운 캔들 데이터를 DataFrame에 효율적으로 추가하고,
        실시간으로 업데이트되는 현재 캔들을 정확하게 반영
        """
        if not candle_deque:
            return False
            
        latest_candle = candle_deque[-1]
        
        # DataFrame이 비어있으면 전체 deque로 초기화
        if self.df.empty:
            self.df = pd.DataFrame(list(candle_deque))
            # 초기화 후 타입 변환을 한 번 수행
            self.df = self.df.astype({
                'open': 'float',
                'high': 'float',
                'low': 'float',
                'close': 'float',
                'volume': 'float'
            })
        # DataFrame에 데이터가 있을 경우
        else:
            # 기존 캔들 업데이트 (타임스탬프가 마지막 행과 동일)
            if self.df.iloc[-1]['timestamp'] == latest_candle['timestamp']:
                # 마지막 행의 데이터를 최신 캔들 정보로 덮어쓰기
                self.df.iloc[-1, self.df.columns.get_loc('high')] = latest_candle['high']
                self.df.iloc[-1, self.df.columns.get_loc('low')] = latest_candle['low']
                self.df.iloc[-1, self.df.columns.get_loc('close')] = latest_candle['close']
                self.df.iloc[-1, self.df.columns.get_loc('volume')] = latest_candle['volume']
            
            # 새로운 캔들 추가 (타임스탬프가 변경됨)
            else:
                new_row = pd.DataFrame([latest_candle])
                self.df = pd.concat([self.df, new_row], ignore_index=True)

        # 오래된 데이터 제거
        if len(self.df) > self.max_data_length:
            self.df = self.df.iloc[-self.max_data_length:].reset_index(drop=True)
            
        return True

    def _calculate_vpoc(self, df: pd.DataFrame, bin_size_multiplier: float = 0.001) -> Optional[float]:
        """
        캔들의 고가/저가 범위를 모두 고려하여 더 정확한 VPOC(거래량 최다 집중 가격대)를 계산
        """
        if df.empty or len(df) < 2:
            return None

        last_close = df['close'].iloc[-1]
        bin_size = last_close * bin_size_multiplier
        if bin_size <= 0:
            return None

        # 가격대별 거래량을 저장할 딕셔너리
        volume_profile = {}

        for _, row in df.iterrows():
            low_bin = int(row['low'] / bin_size)
            high_bin = int(row['high'] / bin_size)
            num_bins = high_bin - low_bin + 1
            
            # 캔들 하나의 거래량을 해당 캔들이 포함하는 가격대에 균등하게 분배
            volume_per_bin = row['volume'] / num_bins if num_bins > 0 else 0

            for bin_index in range(low_bin, high_bin + 1):
                price_level = bin_index * bin_size
                volume_profile[price_level] = volume_profile.get(price_level, 0) + volume_per_bin
        
        if not volume_profile:
            return None
        
        # 거래량이 가장 많은 가격대를 반환
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

        # 데이터 무결성 및 최소 개수 확인
        if len(base_candle_deque) < config.LONG_TERM_MA_WINDOW + 1:
            return
        
        # DataFrame 업데이트 로직 호출
        if not self._update_dataframe(base_candle_deque):
            return 

        # 지표 계산
        self.df['rsi'] = ta.momentum.RSIIndicator(self.df['close'], window=config.RSI_WINDOW).rsi()
        self.df['ma_long'] = ta.trend.sma_indicator(self.df['close'], window=config.LONG_TERM_MA_WINDOW)
        
        latest = self.df.iloc[-1]
        if pd.isna(latest['rsi']) or pd.isna(latest['ma_long']):
            return

        # 시장 컨텍스트 정의
        current_trend = "상승" if latest['close'] > latest['ma_long'] else "하락"

        # VPOC 계산
        recent_data_for_vpoc = self.df.tail(config.MARKET_STRUCTURE_CANDLE_COUNT)
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

        elif self.ranking_manager.is_seeking('low') and latest['rsi'] < config.RSI_EXIT_THRESHOLD:
            current_low_session = self.ranking_manager.get_current_session_info('low')
            if current_low_session and latest_price < current_low_session['price']:
                last_low_point = self.divergence_tracker['low']['last_point']
                if last_low_point and latest_price < last_low_point['price'] and indicator_data['rsi'] > last_low_point['indicators']['rsi']:
                    div_msg = (f"🔥🔥🔥 강세 다이버전스 출현 가능성! 🔥🔥🔥\n"
                               f" - 가격: {last_low_point['price']:,.0f}원 -> {latest_price:,.0f}원 (하락)\n"
                               f" - RSI: {last_low_point['indicators']['rsi']:.2f} -> {indicator_data['rsi']:.2f} (상승)")
                    logger.warning(div_msg)
                    send_direct_webhook(div_msg)
                
                self.divergence_tracker['low']['last_point'] = {'price': latest_price, 'indicators': indicator_data, 'timestamp': latest_timestamp}
            self.ranking_manager.update_if_needed('low', latest_price, latest_timestamp, indicator_data)

        elif latest['rsi'] >= config.RSI_EXIT_THRESHOLD and self.ranking_manager.is_seeking('low'):
            final_low_session = self.ranking_manager.get_current_session_info('low')
            if final_low_session:
                rebound_percent = ((latest_price - final_low_session['price']) / final_low_session['price']) * 100
                
                context_info = ""
                if most_traded_level and abs(final_low_session['price'] - most_traded_level) / most_traded_level < (config.KEY_LEVEL_TOLERANCE_PERCENT / 100):
                    context_info = " (🔥핵심 지지 구간에서 반등!)"
                self.ranking_manager.end_seeking('low', rebound_percent, context_info)
            else:
                self.ranking_manager.end_seeking('low', 0.0)

        # 고점 탐색 전략
        if latest['rsi'] >= config.RSI_HIGH_THRESHOLD and not self.ranking_manager.is_seeking('high'):
            context_info = f" ({current_trend} 추세 중)"
            self.ranking_manager.start_seeking('high', latest_price, latest_timestamp, indicator_data, context_info)
            self.divergence_tracker['high']['last_point'] = None
        
        elif self.ranking_manager.is_seeking('high') and latest['rsi'] >= config.RSI_HIGH_THRESHOLD:
            current_high_session = self.ranking_manager.get_current_session_info('high')
            if current_high_session and latest_price > current_high_session['price']:
                last_high_point = self.divergence_tracker['high']['last_point']
                if last_high_point and latest_price > last_high_point['price'] and indicator_data['rsi'] < last_high_point['indicators']['rsi']:
                    div_msg = (f"📉📉📉 약세 다이버전스 출현 가능성! 📉📉📉\n"
                            f" - 가격: {last_high_point['price']:,.0f}원 -> {latest_price:,.0f}원 (상승)\n"
                            f" - RSI: {last_high_point['indicators']['rsi']:.2f} -> {indicator_data['rsi']:.2f} (하락)")
                    logger.warning(div_msg)
                    send_direct_webhook(div_msg)
                
                self.divergence_tracker['high']['last_point'] = {'price': latest_price, 'indicators': indicator_data, 'timestamp': latest_timestamp}
            self.ranking_manager.update_if_needed('high', latest_price, latest_timestamp, indicator_data)

        elif self.ranking_manager.is_seeking('high') and latest['rsi'] <= config.RSI_HIGH_EXIT_THRESHOLD:
            final_high_session = self.ranking_manager.get_current_session_info('high')
            if final_high_session:
                pullback_percent = ((final_high_session['price'] - latest_price) / final_high_session['price']) * 100
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
            logger.warning(ranking_manager.get_final_ranking_board_message())
            shutdown_logger()
            print(f"\n--- {CLIENT_ID}: 모든 작업 완료 ---")