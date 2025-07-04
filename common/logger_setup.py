import logging
import logging.handlers
import os

# 프로젝트 모듈 임포트
import config
from common.webhook_manager import get_webhook_handler, shutdown_webhook_sender, send_direct_webhook

def setup_logger() -> logging.Logger:
    """
    설정 파일(config.py)을 기반으로 콘솔, 파일, 웹훅 핸들러를 포함하는
    종합 로거를 설정하고 반환합니다.
    """
    logger = logging.getLogger(config.APP_LOGGER_NAME)
    if logger.hasHandlers():
        return logger

    logger.setLevel(config.LOG_LEVEL)

    # 통합 포맷터 생성
    formatter = logging.Formatter(config.LOG_FORMAT)

    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(config.CONSOLE_LOG_LEVEL)
    console_handler.setFormatter(formatter) 
    logger.addHandler(console_handler)

    # 파일 핸들러
    if config.ENABLE_FILE_LOGGING:
        log_dir = os.path.dirname(config.LOG_FILE_PATH)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = logging.handlers.RotatingFileHandler(
            filename=config.LOG_FILE_PATH,
            maxBytes=config.LOG_FILE_MAX_BYTES,
            backupCount=config.LOG_FILE_BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(config.FILE_LOG_LEVEL)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # 웹훅 핸들러
    webhook_handler = get_webhook_handler()
    if webhook_handler:
        webhook_handler.setLevel(config.WEBHOOK_LOG_LEVEL)
        webhook_handler.setFormatter(formatter)
        logger.addHandler(webhook_handler)
        logger.info("웹훅 로깅 핸들러가 활성화되었습니다.")

    logger.info(f"'{config.APP_LOGGER_NAME}' 로거 설정 완료.")
    return logger

def shutdown_logger():
    """
    로깅 시스템을 안전하게 종료합니다.
    """
    logger = logging.getLogger(config.APP_LOGGER_NAME)
    logger.info("로깅 시스템 종료 절차 시작...")

    # 웹훅 서비스 종료 요청
    shutdown_webhook_sender()
    
    # 표준 로깅 종료
    logging.shutdown()
    print("[INFO] 로깅 시스템이 종료되었습니다.")