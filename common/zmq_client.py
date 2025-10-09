import zmq
import orjson
import time
import logging
import threading
from typing import Dict, List, Callable, Optional
from collections import deque

from config import (
    ZMQ_GATEWAY_HOST, ZMQ_GATEWAY_REQ_PORT, ZMQ_GATEWAY_PUB_PORT,
    SERVER_CANDLE_TTL, CANDLE_DEQUE_MAXLEN
)

logger = logging.getLogger(__name__)

class ZMQClient:
    """
    ZMQ 게이트웨이와 통신하여 실시간 캔들 데이터를 수신하고 관리하는 클라이언트.

    이 클래스는 다음을 담당합니다:
    1. ZMQ REQ-REP 패턴을 사용해 게이트웨이에 초기 데이터(스냅샷) 및 구독 요청.
    2. ZMQ PUB-SUB 패턴을 사용해 실시간으로 브로드캐스트되는 캔들 데이터 수신.
    3. 수신된 캔들 데이터를 내부 deque에 스레드에 안전하게 저장 및 관리.
    4. 주기적으로 구독을 갱신하여 연결을 유지.
    5. 데이터 업데이트 시 등록된 콜백 함수(전략)를 트리거.
    6. 별도의 스레드에서 모든 네트워크 I/O 및 주기적 작업을 처리하여 메인 스레드를 블로킹하지 않음.
    """
    def __init__(self, client_id: str, symbol: str, intervals: List[str], candle_handler_callback: Callable[[Dict[str, deque]], None], throttle_seconds: Optional[float] = 0.1):
        """
        ZMQClient 인스턴스를 초기화합니다.

        Args:
            client_id (str): 게이트웨이에서 클라이언트를 식별하기 위한 고유 ID.
            symbol (str): 거래할 자산의 심볼 (예: "KRW-BTC").
            intervals (List[str]): 구독할 캔들의 시간 간격 리스트 (예: ["minute1", "minute5"]).
            candle_handler_callback (Callable): 새로운 캔들 데이터가 수신될 때마다 호출될 콜백 함수.
                                                이 함수는 `candle_deques` 딕셔너리를 인자로 받습니다.
            throttle_seconds (Optional[float]): 콜백 함수 호출을 제한하는 시간(초). 
                                                            None이나 0으로 설정하면 제한 없이 즉시 호출됩니다.
                                                            기본값은 0.1초입니다.
        """
        self.client_id = client_id
        self.symbol = symbol
        self.intervals = intervals
        self.candle_handler_callback = candle_handler_callback # 캔들 이벤트 처리 콜백 함수
        self.throttle_seconds = throttle_seconds  

        self.context = zmq.Context()
        self.stop_event = threading.Event()  # 모든 백그라운드 스레드의 종료를 제어하는 이벤트
        self.storage_lock = threading.Lock() # `candle_deques`에 대한 동시 접근을 막기 위한 락
        self.data_updated_event = threading.Event() # 데이터 업데이트 시 전략 실행을 트리거하기 위한 이벤트 (디바운싱용)

        self.consecutive_renewal_failures = 0
        self.MAX_CONSECUTIVE_FAILURES = 3 

        # 각 인터벌별 캔들 데이터를 저장하는 딕셔너리
        self.candle_deques: Dict[str, deque] = {
            interval: deque(maxlen=CANDLE_DEQUE_MAXLEN) for interval in self.intervals
        }
        self.threads: List[threading.Thread] = []

    def _send_request(self, request: dict, max_retries: int = 5, initial_delay: int = 2) -> Optional[dict]:
        """
        ZMQ REQ 소켓을 사용해 게이트웨이에 동기식 요청을 보내고 응답을 받습니다.

        Args:
            request (dict): 게이트웨이로 전송할 요청 메시지 (JSON 직렬화 가능해야 함).

        Returns:
            Optional[dict]: 게이트웨이로부터 받은 응답. 타임아웃 또는 오류 발생 시 None을 반환.
        """
        delay = initial_delay
        for attempt in range(max_retries):
            # 매 시도마다 새로운 소켓을 생성하여 연결 상태를 초기화합니다.
            socket_req = self.context.socket(zmq.REQ)
            socket_req.setsockopt(zmq.RCVTIMEO, 10000) # 타임아웃을 10초로 조금 늘려 안정성 확보
            socket_req.setsockopt(zmq.LINGER, 0)
            socket_req.connect(f"tcp://{ZMQ_GATEWAY_HOST}:{ZMQ_GATEWAY_REQ_PORT}")

            try:
                socket_req.send(orjson.dumps(request))
                response = orjson.loads(socket_req.recv())
                return response  # 성공 시 즉시 응답 반환
            
            except zmq.Again:
                logger.warning(
                    f"ZMQ 게이트웨이 응답 시간 초과 (시도 {attempt + 1}/{max_retries})."
                )
            
            except Exception as e:
                logger.error(
                    f"ZMQ 요청 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}"
                )
            finally:
                socket_req.close()

            # 마지막 시도가 아니면 대기
            if attempt < max_retries - 1:
                logger.info(f"{delay}초 후 재시도합니다...")
                time.sleep(delay)
                delay = min(delay * 2, 30) # 대기 시간을 2배씩 늘리되, 최대 30초까지만

        logger.error(f"최대 재시도 횟수({max_retries}회)를 초과하여 요청에 최종 실패했습니다: {request}")
        return None


    def _handle_candle_event(self, topic_str: str, payload: dict):
        """
        수신된 캔들 이벤트를 파싱하고 내부 데이터 저장소(`candle_deques`)를 업데이트합니다.

        이벤트 유형:
        - UPDATE: 현재 진행 중인 캔들의 정보 (시가, 고가, 저가, 종가)가 업데이트됨.
        - CLOSE: 캔들이 마감되고 새로운 캔들이 시작됨.
        - RECONCILE: 거래소의 최종 데이터와 불일치가 있을 경우, 과거 캔들 데이터가 보정됨.

        Args:
            topic_str (str): 이벤트가 발생한 ZMQ 토픽 문자열.
            payload (dict): 이벤트와 관련된 데이터.
        """
        _, symbol, interval, event_type = topic_str.split(':')

        if interval not in self.candle_deques:
            return

        target_deque = self.candle_deques[interval]

        with self.storage_lock:
            if event_type == "UPDATE" and target_deque:
                target_deque[-1] = payload
            elif event_type == "CLOSE" and target_deque:
                target_deque[-1] = payload["closed"] # 마감된 캔들 확정
                target_deque.append(payload["new"])  # 새로 시작된 캔들 추가
            elif event_type == "RECONCILE":
                # 지연된 데이터 등으로 과거 캔들 데이터가 보정될 때 처리
                reconciled_candle = payload
                for i in range(len(target_deque) - 1, -1, -1):
                    if target_deque[i]['timestamp'] == reconciled_candle['timestamp']:
                        logger.info(f"\n[INFO] [{interval}] 캔들 데이터 보정 발생! T:{reconciled_candle['timestamp']}")
                        target_deque[i] = reconciled_candle
                        break
        
        # 데이터가 업데이트되었음을 다른 스레드에 알림
        self.data_updated_event.set()

    def _data_listener_thread(self):
        """[스레드 타겟] ZMQ SUB 소켓을 통해 실시간 캔들 데이터를 구독하고 수신합니다."""
        socket_sub = self.context.socket(zmq.SUB)
        socket_sub.connect(f"tcp://{ZMQ_GATEWAY_HOST}:{ZMQ_GATEWAY_PUB_PORT}")

        for interval in self.intervals:
            topic = f"CANDLE:{self.symbol}:{interval}:"
            socket_sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            logger.info(f"[{self.client_id}][SUB] 토픽 구독: '{topic}'")

        # stop_event가 설정될 때까지 non-blocking 방식으로 메시지를 계속 수신
        while not self.stop_event.is_set():
            try:
                topic_bytes, payload_bytes = socket_sub.recv_multipart(flags=zmq.NOBLOCK)
                self._handle_candle_event(topic_bytes.decode(), orjson.loads(payload_bytes))
            except zmq.Again:
                # 메시지가 없을 경우 CPU 사용을 줄이기 위해 잠시 대기
                time.sleep(0.01)
        socket_sub.close()
        logger.debug(f"\n[{self.client_id}][SUB] 데이터 리스너 종료.")

    def _subscription_renewer_thread(self):
        """[스레드 타겟] 게이트웨이의 구독 TTL이 만료되기 전에 주기적으로 구독을 갱신합니다."""
        # 서버의 TTL 절반보다 조금 짧은 주기로 갱신하여 안정성 확보
        renew_interval = (SERVER_CANDLE_TTL / 2) - 10
        if renew_interval < 10: renew_interval = 10

        while not self.stop_event.wait(renew_interval):
            logger.info(f"모든 캔들 구독 갱신을 시작합니다...")
            
            all_renewals_succeeded = True
            
            for interval in self.intervals:
                request = {
                    "action": "subscribe_candle", 
                    "symbol": self.symbol, 
                    "interval": interval
                }
                response = self._send_request(request)
                
                if not (response and response.get('status') == 'ok'):
                    all_renewals_succeeded = False
                    logger.error(f"❌ [{interval}] 구독 갱신에 최종 실패했습니다! 응답: {response}")

            # interval 갱신 후
            if all_renewals_succeeded:
                if self.consecutive_renewal_failures > 0:
                    logger.info(
                        f"✅ 구독 갱신이 정상화되었습니다. (이전 연속 실패: {self.consecutive_renewal_failures}회)"
                    )
                self.consecutive_renewal_failures = 0
            else:
                self.consecutive_renewal_failures += 1
                logger.warning(
                    f"⚠️ 구독 갱신 주기에 실패가 포함되었습니다. (현재 연속 {self.consecutive_renewal_failures}회 실패)"
                )
                
                # 연속 실패
                if self.consecutive_renewal_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    critical_msg = (
                        f"🚨🚨🚨 [심각] ZMQ 구독 갱신이 {self.consecutive_renewal_failures}회 연속 실패했습니다!\n"
                        f"- 클라이언트 ID: {self.client_id}\n"
                        f"- 서버와의 연결이 끊겼을 가능성이 매우 높습니다.\n"
                        f"- 데이터 수신이 중단되었을 수 있으니 서버 및 네트워크 상태를 즉시 확인하세요."
                    )
                    logger.critical(critical_msg)
                    
                    try:
                        from common.logger_setup import send_direct_webhook
                        send_direct_webhook(critical_msg)
                    except ImportError:
                        logger.error("웹훅 발송에 실패했습니다. (send_direct_webhook 함수를 찾을 수 없음)")
                    except Exception as e:
                        logger.error(f"웹훅 발송 중 예외 발생: {e}")
        logger.debug(f"[{self.client_id}][RENEWER] 구독 갱신 스레드 종료.")

    def _strategy_trigger_thread(self):
        """
        [스레드 타겟] 데이터 업데이트 이벤트를 감지하여 전략 콜백 함수를 실행합니다.
        ...
        """
        if self.throttle_seconds and self.throttle_seconds > 0:
            while not self.stop_event.wait(self.throttle_seconds):
                if self.data_updated_event.is_set():
                    self.data_updated_event.clear()
                    try:
                        with self.storage_lock:
                            self.candle_handler_callback(self.candle_deques)
                    except Exception as e:
                        logger.error(f"전략 콜백 함수 실행 중 오류: {e}", exc_info=True)
        else:
            while not self.stop_event.is_set():
                if self.data_updated_event.wait(timeout=1): 
                    self.data_updated_event.clear()
                    try:
                        with self.storage_lock:
                            self.candle_handler_callback(self.candle_deques)
                    except Exception as e:
                        logger.error(f"전략 콜백 함수 실행 중 오류: {e}", exc_info=True)

        logger.debug(f"[{self.client_id}][TRIGGER] 전략 트리거 스레드 종료.")

    def start(self) -> bool:
        """
        클라이언트를 시작합니다.

        초기 캔들 데이터(스냅샷)를 요청하고, 성공적으로 수신하면
        데이터 수신, 구독 갱신, 전략 실행을 위한 백그라운드 스레드를 시작합니다.

        Returns:
            bool: 초기화 및 스레드 시작에 성공하면 True, 실패하면 False.
        """
        logger.info("ZMQ 게이트웨이에 연결 및 스냅샷 요청을 시작합니다...")

        # 모든 인터벌에 대해 과거 데이터 스냅샷을 먼저 요청
        for interval in self.intervals:
            req = {"action": "subscribe_candle", "symbol": self.symbol, "interval": interval, "history_count": CANDLE_DEQUE_MAXLEN}
            response = self._send_request(req)
            
            if response and response.get('status') == 'ok' and response.get('data'):
                with self.storage_lock:
                    self.candle_deques[interval].extend(response['data'])
                logger.info(f"✅ [{interval}] 스냅샷 수신 성공 ({len(response['data'])}개).")
            else:
                logger.error(f"❌ [{interval}] 스냅샷 수신 실패. 클라이언트를 시작할 수 없습니다.")
                return False

        # 모든 스냅샷 수신 성공 후 백그라운드 스레드들 시작
        listener = threading.Thread(target=self._data_listener_thread, name="ZMQListener")
        renewer = threading.Thread(target=self._subscription_renewer_thread, name="SubscriptionRenewer")
        trigger = threading.Thread(target=self._strategy_trigger_thread, name="StrategyTrigger")
        self.threads.extend([listener, renewer, trigger])
        for t in self.threads:
            t.start()
        
        logger.info("✅ 모든 스냅샷 수신 완료. 실시간 분석을 시작합니다.")
        return True

    def stop(self):
        """
        클라이언트를 안전하게 종료합니다.

        모든 백그라운드 스레드에 종료 신호를 보내고, 게이트웨이에 구독 해지를
        요청한 후, 스레드가 종료될 때까지 대기합니다.
        """
        logger.info("ZMQ 클라이언트 종료 절차 시작...")
        self.stop_event.set()

        # 게이트웨이에 더 이상 데이터를 받지 않겠다고 알림
        for interval in self.intervals:
            logger.debug(f"[{interval}] 구독 해지 요청 중...")
            self._send_request({"action": "unsubscribe_candle", "symbol": self.symbol, "interval": interval})
        
        # 모든 스레드가 종료될 때까지 대기
        for t in self.threads:
            if t.is_alive():
                t.join(timeout=5)
        
        self.context.term() # ZMQ 컨텍스트 정리
        logger.info("ZMQ 클라이언트가 성공적으로 종료되었습니다.")