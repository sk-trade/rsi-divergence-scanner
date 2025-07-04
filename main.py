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
ranking_manager = RankingManager() 
divergence_tracker = {
    'low': {'last_point': None},
    'high': {'last_point': None} 
}

def strategy_analyzer(candle_deques: Dict[str, deque]):
    """
    캔들 데이터를 분석하여 시장 구조 및 추세를 파악하고,
    그 컨텍스트 안에서 저점/고점 신호를 평가하여 RankingManager에게 명령합니다.
    """
    global divergence_tracker
    try:
        base_candle_deque = candle_deques[config.BASE_INTERVAL]
    except KeyError:
        return

    # --- 데이터 무결성 및 최소 개수 확인 ---
    # 가장 긴 분석 기간(장기 이평선)을 기준으로 최소 캔들 개수 확인
    if len(base_candle_deque) < config.LONG_TERM_MA_WINDOW + 1:
        return
    
    # --- Phase 1: 데이터 준비 및 지표 계산 ---
    df = pd.DataFrame(list(base_candle_deque))
    try:
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)
    except (KeyError, TypeError) as e:
        logger.error(f"데이터프레임 처리 중 필수 컬럼 오류: {e}")
        return

    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=config.RSI_WINDOW).rsi()
    df['ma_long'] = ta.trend.sma_indicator(df['close'], window=config.LONG_TERM_MA_WINDOW)
    
    latest = df.iloc[-1]
    # 필수 지표 계산 실패 시 분석 중단
    if pd.isna(latest['rsi']) or pd.isna(latest['ma_long']):
        return

    # --- Phase 2: 시장 컨텍스트 정의 ---
    # 2-A: 추세 판단
    current_trend = "상승" if latest['close'] > latest['ma_long'] else "하락"

    # 2-B: 시장 구조 분석 (VPOC 근사치 계산)
    most_traded_level = None
    # 시장 구조 분석은 설정된 기간만큼 데이터가 쌓였을 때만 수행
    df['mid_price'] = (df['high'] + df['low']) / 2
    recent_data = df.tail(config.MARKET_STRUCTURE_CANDLE_COUNT)
    
    bin_size = latest['close'] * 0.005
    if bin_size > 0:
        price_level_key = round(recent_data['mid_price'] / bin_size)
        volume_by_level = recent_data.groupby(price_level_key)['volume'].sum()
        
        if not volume_by_level.empty:
            most_traded_level_key = volume_by_level.idxmax()
            most_traded_level = most_traded_level_key * bin_size

    # --- 3. 상태 메시지 및 데이터 준비 ---
    status_msg = (f"\r[{config.SYMBOL}:{config.BASE_INTERVAL}] "
                  f"Close: {latest['close']:<10,.0f} | "
                  f"RSI: {latest['rsi']:.2f} | "
                  f"추세: {current_trend} | "
                  f"탐색(L/H): {'ON' if ranking_manager.is_seeking('low') else 'OFF'}/{'ON' if ranking_manager.is_seeking('high') else 'OFF'}{' '*5}")
    print(status_msg, end="")

    latest_price = latest['close']
    latest_timestamp = latest['timestamp']
    indicator_data = { 'rsi': latest['rsi'] }

    # --- 4. 전략적 의사결정 (컨텍스트 결합) ---
    # --- 저점 탐색 전략 ---
    if latest['rsi'] <= config.RSI_LOW_THRESHOLD and not ranking_manager.is_seeking('low'):
        context_info = f" ({current_trend} 추세 중)"
        ranking_manager.start_seeking('low', latest_price, latest_timestamp, indicator_data, context_info)
        divergence_tracker['low']['last_point'] = None

    elif ranking_manager.is_seeking('low') and latest['rsi'] < config.RSI_EXIT_THRESHOLD:
        current_low_session = ranking_manager.get_current_session_info('low')
        if current_low_session and latest_price < current_low_session['price']:
            last_low_point = divergence_tracker['low']['last_point']
            if last_low_point and latest_price < last_low_point['price'] and indicator_data['rsi'] > last_low_point['indicators']['rsi']:
                div_msg = (f"🔥🔥🔥 강세 다이버전스 출현 가능성! 🔥🔥🔥\n"
                           f" - 가격: {last_low_point['price']:,.0f}원 -> {latest_price:,.0f}원 (하락)\n"
                           f" - RSI: {last_low_point['indicators']['rsi']:.2f} -> {indicator_data['rsi']:.2f} (상승)")
                logger.warning(div_msg)
                send_direct_webhook(div_msg)
            
            divergence_tracker['low']['last_point'] = {'price': latest_price, 'indicators': indicator_data, 'timestamp': latest_timestamp}
        ranking_manager.update_if_needed('low', latest_price, latest_timestamp, indicator_data)

    elif latest['rsi'] >= config.RSI_EXIT_THRESHOLD and ranking_manager.is_seeking('low'):
        final_low_session = ranking_manager.get_current_session_info('low')
        if final_low_session:
            rebound_percent = ((latest_price - final_low_session['price']) / final_low_session['price']) * 100
            
            context_info = ""
            if most_traded_level and abs(final_low_session['price'] - most_traded_level) / most_traded_level < (config.KEY_LEVEL_TOLERANCE_PERCENT / 100):
                context_info = " (🔥핵심 지지 구간에서 반등!)"
            ranking_manager.end_seeking('low', rebound_percent, context_info)
        else:
            ranking_manager.end_seeking('low', 0.0)

    # --- 고점 탐색 전략 ---
    if latest['rsi'] >= config.RSI_HIGH_THRESHOLD and not ranking_manager.is_seeking('high'):
        context_info = f" ({current_trend} 추세 중)"
        ranking_manager.start_seeking('high', latest_price, latest_timestamp, indicator_data, context_info)
        divergence_tracker['high']['last_point'] = None
    
    elif ranking_manager.is_seeking('high') and latest['rsi'] > config.RSI_HIGH_EXIT_THRESHOLD:
        ranking_manager.update_if_needed('high', latest_price, latest_timestamp, indicator_data)

    elif latest['rsi'] <= config.RSI_HIGH_EXIT_THRESHOLD and ranking_manager.is_seeking('high'):
        final_high_session = ranking_manager.get_current_session_info('high')
        if final_high_session:
            pullback_percent = ((final_high_session['price'] - latest_price) / final_high_session['price']) * 100
            
            context_info = ""
            if most_traded_level and abs(final_high_session['price'] - most_traded_level) / most_traded_level < (config.KEY_LEVEL_TOLERANCE_PERCENT / 100):
                context_info = " (⚠️핵심 저항 구간에서 하락!)"
            ranking_manager.end_seeking('high', pullback_percent, context_info)
        else:
            ranking_manager.end_seeking('high', 0.0)

if __name__ == "__main__":
    start_msg = f"--- {CLIENT_ID}: '차트상 최저점' 분석기 시작 ---"
    logger.info(start_msg)
    
    client = ZMQClient(
        client_id=CLIENT_ID,
        symbol=config.SYMBOL,
        intervals=config.INTERVALS,
        candle_handler_callback=strategy_analyzer
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