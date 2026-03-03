import os
import logging

# -- logging 설정 --
APP_LOGGER_NAME = "LowPointFinder"
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '%(asctime)s - %(levelname)-8s - %(name)s - %(message)s'
# 콘솔 핸들러 설정
CONSOLE_LOG_LEVEL = logging.INFO
# 파일 핸들러 설정
ENABLE_FILE_LOGGING = True
FILE_LOG_LEVEL = logging.DEBUG
LOG_FILE_PATH = "logs/low_point.log"
LOG_FILE_MAX_BYTES = 1 * 1024 * 1024  
LOG_FILE_BACKUP_COUNT = 1
# 웹훅 핸들러 설정
WEBHOOK_URL = os.getenv("WEBHOOK_URL", 'url.com')
WEBHOOK_LOG_LEVEL = logging.WARNING

# --- ZMQ 설정 ---
ZMQ_GATEWAY_HOST = os.getenv("ZMQ_GATEWAY_HOST", "localhost")
ZMQ_GATEWAY_REQ_PORT = os.getenv("ZMQ_GATEWAY_REQ_PORT", "11556")
ZMQ_GATEWAY_PUB_PORT = os.getenv("ZMQ_GATEWAY_PUB_PORT", "11558")
SERVER_CANDLE_TTL = int(os.getenv('CANDLE_SUBSCRIPTION_TTL_SECONDS', '300'))
ZMQ_THROTTLE_SECONDS = 0.1

# --- 분석 대상 및 전략 설정 ---
SYMBOL = "KRW-BTC"
EXCHANGE = os.getenv("EXCHANGE", "upbit")
INTERVALS = ["1h"]
BASE_INTERVAL = "1h"

# RSI 기반 탐색 전략 설정
RSI_WINDOW = 14
RSI_LOW_THRESHOLD = 30  # 탐색 시작 RSI 기준
RSI_EXIT_THRESHOLD = 35 # 탐색 종료 RSI 기준 (채터링 방지)
RSI_HIGH_THRESHOLD = 70 # 탐색 시작 RSI 기준
RSI_HIGH_EXIT_THRESHOLD = 65 # 탐색 종료 RSI 기준 (채터링 방지)
# --- 랭킹 시스템 설정 ---
RANKING_DATA_FILE = "data/ranking_data.json"
RANKING_MAX_COUNT = 100 # 랭킹 보드에 저장할 최대 개수
RANKING_VALID_DAYS = 7  # 랭킹 데이터 및 캔들 데이터의 유효 기간 (일)
NOTIFICATION_COOLDOWN_SECONDS = 10 # 웹훅 쿨다운

# 추세 판단 및 시장 구조 분석 설정
LONG_TERM_MA_WINDOW = 60  # 추세 판단을 위한 장기 이동평균선 기간
KEY_LEVEL_TOLERANCE_PERCENT = 1.0 # 핵심 지지/저항 레벨로 간주할 가격 근접 허용 오차 (%)

# --- 데이터 관리 설정 ---
# 랭킹 유효 기간에 맞춰 필요한 캔들 개수 자동 계산
try:
    if BASE_INTERVAL.endswith("m"):
        minutes = int(BASE_INTERVAL[:-1])
        CANDLES_PER_DAY = (24 * 60) // minutes
    elif BASE_INTERVAL.endswith("h"):
        hours = int(BASE_INTERVAL[:-1])
        CANDLES_PER_DAY = 24 // hours
    elif BASE_INTERVAL.endswith("d"):
        CANDLES_PER_DAY = 1
    else:
        CANDLES_PER_DAY = 48
except (ValueError, TypeError):
    CANDLES_PER_DAY = 48
    
# 시장 구조 분석에 사용할 캔들 개수를 RANKING_VALID_DAYS 기준으로 자동 계산
MARKET_STRUCTURE_CANDLE_COUNT = CANDLES_PER_DAY * RANKING_VALID_DAYS

# RSI 계산을 위해 추가 여유분 확보
# deque의 최대 길이는 시장 구조 분석 기간과 장기 이평선 기간 중 더 긴 것을 기준으로 설정
CANDLE_DEQUE_MAXLEN = max(MARKET_STRUCTURE_CANDLE_COUNT, LONG_TERM_MA_WINDOW) + RSI_WINDOW + 5 # 넉넉한 버퍼

