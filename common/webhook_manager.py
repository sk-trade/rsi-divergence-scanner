import asyncio
import aiohttp
import logging
import os
import threading
from queue import Queue, Empty
from typing import Optional

import config
_webhook_queue = Queue()
_async_loop: Optional[asyncio.AbstractEventLoop] = None
_async_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()

async def _send_webhook_message(message: str) -> bool:
    """비동기로 웹훅 메시지를 전송합니다."""
    webhook_url = config.WEBHOOK_URL
    if not webhook_url:
        print(f"[CRITICAL] Webhook URL이 설정되지 않았습니다.")
        return False

    payload = {"text": message}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload, timeout=10) as response:
                if response.status == 200:
                    return True
                else:
                    error_text = await response.text()
                    print(f"[ERROR] Webhook 전송 실패: {response.status} - {error_text}")
                    return False
    except Exception as e:
        print(f"[ERROR] Webhook 전송 중 예외 발생: {e}")
        return False

async def _webhook_sender_task():
    """큐에서 메시지를 꺼내 비동기적으로 웹훅으로 전송하는 코루틴"""
    while not _stop_event.is_set() or not _webhook_queue.empty():
        try:
            message = _webhook_queue.get_nowait()
            await _send_webhook_message(message)
            _webhook_queue.task_done()
        except Empty:
            await asyncio.sleep(0.1)
        except Exception as e:
            print(f"[ERROR] 웹훅 발신 작업(큐 처리) 중 예외 발생: {e}")
            await asyncio.sleep(1)

def _asyncio_event_loop_thread_entry():
    """웹훅 전송을 위한 비동기 이벤트 루프를 실행하는 스레드"""
    global _async_loop
    try:
        _async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_async_loop)
        _async_loop.run_until_complete(_webhook_sender_task())
    finally:
        if _async_loop and _async_loop.is_running():
            _async_loop.close()
        _async_loop = None
        print("[INFO] 웹훅 전송 스레드 종료.")

class WebhookQueueHandler(logging.Handler):
    """로그 레코드를 내부 큐에 넣는 커스텀 핸들러."""
    def __init__(self, queue: Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        self.queue.put(self.format(record))

def get_webhook_handler() -> Optional[WebhookQueueHandler]:
    """
    웹훅 핸들러를 생성하고, 필요 시 백그라운드 스레드를 시작합니다.
    웹훅 URL이 없으면 None을 반환합니다.
    """
    global _async_thread
    if not config.WEBHOOK_URL:
        return None

    if _async_thread is None or not _async_thread.is_alive():
        _stop_event.clear()
        _async_thread = threading.Thread(
            target=_asyncio_event_loop_thread_entry,
            name="WebhookSenderThread",
            daemon=True
        )
        _async_thread.start()
        print("[INFO] 웹훅 전송 서비스가 시작되었습니다.")

    return WebhookQueueHandler(_webhook_queue)

def send_direct_webhook(message: str):
    """
    로깅 시스템을 거치지 않고 웹훅 큐에 직접 메시지를 보냅니다.
    웹훅 서비스가 시작되지 않았다면 아무 동작도 하지 않습니다.
    """
    if _async_thread and _async_thread.is_alive():
        _webhook_queue.put(message)

def shutdown_webhook_sender():
    """
    웹훅 전송 서비스를 안전하게 종료합니다. (큐 처리 및 스레드 종료)
    """
    global _async_thread
    if _async_thread and _async_thread.is_alive():
        print(f"[INFO] 웹훅 큐에 남은 메시지 처리 중 ({_webhook_queue.qsize()}개)...")
        _webhook_queue.join()

        _stop_event.set()
        _async_thread.join(timeout=5)
        if _async_thread.is_alive():
            print("[WARNING] 웹훅 전송 스레드가 시간 내에 종료되지 않았습니다.")
    _async_thread = None