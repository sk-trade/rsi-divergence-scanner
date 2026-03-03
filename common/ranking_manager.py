import threading
import time
import pandas as pd
import logging
import os 
import orjson 

import config
from common.logger_setup import send_direct_webhook

logger = logging.getLogger(__name__)

class RankingManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.filepath = config.RANKING_DATA_FILE
        self.directory = os.path.dirname(self.filepath)
        
        self.boards = {'low': [], 'high': []}
        self.seeking_status = {'low': False, 'high': False}
        self.current_sessions = {'low': None, 'high': None}
        
        self.last_update_time = 0
        self.last_notified_ranks = {'low': None, 'high': None}
        
        self.stop_event = threading.Event()
        self.manager_thread = None

        self._load_boards()

    def _to_datetime(self, item):
        ts_value = item.get('ts') if item.get('ts') is not None else item.get('timestamp')
        if isinstance(ts_value, (int, float)):
            return pd.to_datetime(ts_value, unit='ms', utc=True)
        return pd.to_datetime(ts_value, utc=True)

    def _load_boards(self):
        """프로그램 시작 시 파일에서 랭킹 보드를 불러옵니다."""
        try:
            with open(self.filepath, 'rb') as f:
                data = orjson.loads(f.read())
                if 'low' in data and 'high' in data:
                    with self.lock:
                        self.boards = data
                    logger.info(f"✅ 랭킹 데이터를 '{self.filepath}'에서 성공적으로 불러왔습니다.")
                else:
                    logger.warning(f"'{self.filepath}' 파일의 형식이 올바르지 않습니다. 새 랭킹으로 시작합니다.")
        except FileNotFoundError:
            logger.info("랭킹 데이터 파일이 없습니다. 새 랭킹으로 시작합니다.")
        except orjson.JSONDecodeError:
            logger.error(f"'{self.filepath}' 파일의 JSON 형식이 손상되었습니다. 새 랭킹으로 시작합니다.")
        except Exception as e:
            logger.error(f"랭킹 데이터 로드 중 오류 발생: {e}")

    def _save_boards(self):
        """현재 랭킹 보드를 파일에 저장합니다."""
        try:
            if not os.path.exists(self.directory):
                os.makedirs(self.directory)
                logger.info(f"랭킹 데이터 디렉토리 '{self.directory}'를 생성했습니다.")

            with self.lock:
                with open(self.filepath, 'wb') as f:
                    f.write(orjson.dumps(self.boards))
            logger.debug("랭킹 데이터를 파일에 저장했습니다.")
        except Exception as e:
            logger.error(f"랭킹 데이터 저장 중 오류 발생: {e}", exc_info=True)

    def is_seeking(self, mode: str) -> bool:
        """현재 해당 모드(low/high)로 탐색 중인지 확인합니다."""
        return self.seeking_status.get(mode, False)

    def get_current_session_info(self, mode: str):
        """현재 탐색 중인 세션 정보를 반환합니다."""
        with self.lock:
            return self.current_sessions[mode].copy() if self.current_sessions[mode] else None

    def _format_board(self, board, board_type: str, highlight_index=-1):
        """랭킹 보드를 예쁘게 포맷하는 내부 헬퍼 함수"""
        if not board: 
            return f"  (현재 {'저점' if board_type == 'low' else '고점'} 랭킹 데이터 없음)"
        lines = []
        display_board = board[:config.RANKING_MAX_COUNT]

        for i, item in enumerate(display_board):
            ts_value = item.get('ts') if item.get('ts') is not None else item.get('timestamp')
            if isinstance(ts_value, (int, float)):
                dt = pd.to_datetime(ts_value, unit='ms', utc=True)
            else:
                dt = pd.to_datetime(ts_value, utc=True)
            dt = dt.tz_convert('Asia/Seoul')
            dt_str = dt.strftime('%m-%d %H:%M')
            indicators_str = ", ".join([f"{key.upper()}:{val:.2f}" for key, val in item.get('indicators', {}).items()])
            
            line = f"  {i+1: >2}위: {item['price']:,.0f}원 ({indicators_str}) @{dt_str}"
            if i == highlight_index:
                line = f"➡️{line} [★ 이 항목]"
            lines.append(line)
            
        if len(board) > config.RANKING_MAX_COUNT:
            lines.append(f"  ... (총 {len(board)}개)")
        return "\n".join(lines)

    def _get_potential_rank_index(self, session_to_check, board):
        """잠재적 랭킹 인덱스를 계산하는 내부 헬퍼 함수"""
        if not session_to_check: return None
        
        with self.lock:
            board = self.boards[board]
            reverse_sort = (board == 'high')
            temp_board = sorted(board + [session_to_check], key=lambda x: x['price'], reverse=reverse_sort)
        try:
            return temp_board.index(session_to_check)
        except ValueError:
            return None

    def start_seeking(self, mode: str, price: float, timestamp, indicator_data: dict, context_info: str = ""):
        """저점 또는 고점 탐색을 시작합니다."""
        with self.lock:
            self.seeking_status[mode] = True
            self.current_sessions[mode] = {'price': price, 'indicators': indicator_data, 'timestamp': timestamp, 'ts': timestamp}
            self.last_update_time = 0
            self.last_notified_ranks[mode] = None
        
        type_str = "저점" if mode == 'low' else "고점"
        emoji = "🚨" if mode == 'low' else "📈"
        msg = (f"{emoji} {type_str} 탐색 시작!{context_info}\n"
               f" - 시작가: {price:,.0f}원 (지표: {indicator_data})")
        logger.info(msg)
        send_direct_webhook(msg)

    def update_if_needed(self, mode: str, price: float, timestamp, indicator_data: dict):
        """탐색 중인 세션을 갱신할 필요가 있는지 확인하고 처리합니다."""
        with self.lock:
            if not self.seeking_status.get(mode): return
            
            # 저점은 더 낮은 가격, 고점은 더 높은 가격일 때만 갱신
            is_new_extreme = (price < self.current_sessions[mode]['price'] if mode == 'low' 
                              else price > self.current_sessions[mode]['price'])
            if not is_new_extreme: return

            old_price = self.current_sessions[mode]['price']
            self.current_sessions[mode].update({'price': price, 'indicators': indicator_data, 'timestamp': timestamp, 'ts': timestamp})
            type_str = "저점" if mode == 'low' else "고점"
            logger.info(f"{type_str} 갱신됨: {old_price:,.0f} -> {price:,.0f} (로그 전용)")

            # 공통 쿨다운 알림
            current_time = time.time()
            if (current_time - self.last_update_time) > config.NOTIFICATION_COOLDOWN_SECONDS:
                volume_ratio = indicator_data.get('volume_ratio', 1.0)
                volume_info = f" (거래량 급증! x{volume_ratio:.1f})" if volume_ratio > 2.0 else ""
                emoji = "📉" if mode == 'low' else "📈"
                msg = (f"{emoji} {type_str} 갱신!{volume_info}\n"
                       f" - 이전: {old_price:,.0f}원 -> 현재: {price:,.0f}원 (지표: {indicator_data})")
                logger.info(msg)
                send_direct_webhook(msg)
                self.last_update_time = current_time

            # 잠재 랭킹 알림 로직 복원 및 통합
            reverse_sort = (mode == 'high')
            temp_board = sorted(self.boards[mode] + [self.current_sessions[mode]], key=lambda x: x['price'], reverse=reverse_sort)
            potential_rank_idx = temp_board.index(self.current_sessions[mode])
            if potential_rank_idx != self.last_notified_ranks[mode] and potential_rank_idx < config.RANKING_MAX_COUNT:
                title = f"👀 잠재 {type_str} 랭킹 상승! ({potential_rank_idx + 1}위)"
                rank_msg = (f"{title}\n"
                            f" - 현재가: {self.current_sessions[mode]['price']:,.0f}원\n\n"
                            f"--- 가상 {type_str} TOP 10 ---\n"
                            f"{self._format_board(temp_board, board_type=mode, highlight_index=potential_rank_idx)}")
                logger.info(rank_msg)
                send_direct_webhook(rank_msg)
                self.last_notified_ranks[mode] = potential_rank_idx

    def end_seeking(self, mode: str, change_percent: float, context_info: str = ""):
        """저점 또는 고점 탐색을 종료합니다."""
        saved = False
        with self.lock:
            if not self.seeking_status.get(mode): return
            
            final_session = self.current_sessions[mode].copy()
            self.seeking_status[mode] = False
            self.current_sessions[mode] = None
            
            type_str, reverse_sort = ("저점", False) if mode == 'low' else ("고점", True)
            
            # 반등/하락률 계산 및 메시지 생성
            if mode == 'low':
                change_info = f" ({change_percent:+.2f}%)"
                if change_percent > 1.0: change_info = f" (🔥 강력한 V자 반등!{change_info})"
                else: change_info = f" (반등 성공{change_info})"
            else: # mode == 'high'
                change_info = f" ({change_percent:.2f}%)"
                if change_percent > 1.0: change_info = f" (⚠️ 급락 반전!{change_info})"
                else: change_info = f" (하락 전환{change_info})"
            
            # 랭킹 등록
            temp_board = sorted(self.boards[mode] + [final_session], key=lambda x: x['price'], reverse=reverse_sort)
            final_rank_index = temp_board.index(final_session)
            if final_rank_index < config.RANKING_MAX_COUNT:
                self.boards[mode] = temp_board[:config.RANKING_MAX_COUNT]
                saved = True
                msg = (f"✅ {type_str} 탐색 종료{change_info}{context_info}\n"
                       f" - 최종 {type_str}: {final_session['price']:,.0f}원 (지표: {final_session['indicators']})\n"
                       f" - 🏆 {type_str} 랭킹 {final_rank_index + 1}위 확정!\n\n"
                       f"--- 현재 {type_str} TOP 10 ---\n"
                       f"{self._format_board(self.boards[mode], board_type=mode, highlight_index=final_rank_index)}")
            else:
                msg = (f"⭕️ {type_str} 탐색 종료{change_info}{context_info}\n"
                       f" - 최종 {type_str}: {final_session['price']:,.0f}원 (지표: {final_session['indicators']})")
            
            logger.info(msg)
            send_direct_webhook(msg)

        if saved:
            self._save_boards()

    def _periodic_cleanup_task(self):
        """주기적으로 오래된 랭킹 데이터를 정리하는 스레드"""
        check_interval_seconds = 3600
        valid_duration_ms = config.RANKING_VALID_DAYS * 24 * 60 * 60 * 1000

        logger.info(f"랭킹 관리자 시작. {config.RANKING_VALID_DAYS}일 이상된 데이터는 제거됩니다.")
        while not self.stop_event.wait(check_interval_seconds):
            with self.lock:
                removed_count = 0
                now_ts = pd.Timestamp.now(tz='UTC')
                
                # 저점 보드 정리
                initial_low_count = len(self.boards['low'])
                self.boards['low'] = [
                    item for item in self.boards['low']
                    if (now_ts - self._to_datetime(item)).total_seconds() * 1000 <= valid_duration_ms
                ]
                removed_count += initial_low_count - len(self.boards['low'])

                # 고점 보드 정리
                initial_high_count = len(self.boards['high'])
                self.boards['high'] = [
                    item for item in self.boards['high']
                    if (now_ts - self._to_datetime(item)).total_seconds() * 1000 <= valid_duration_ms
                ]
                removed_count += initial_high_count - len(self.boards['high'])

            if removed_count > 0:
                logger.info(f"🧹 랭킹 정리: 총 {removed_count}개 제거.")
                self._save_boards()

        logger.info("랭킹 관리자 종료.")

    def start_manager_thread(self):
        """백그라운드 정리 스레드를 시작"""
        if self.manager_thread is None or not self.manager_thread.is_alive():
            self.stop_event.clear()
            self.manager_thread = threading.Thread(target=self._periodic_cleanup_task, name="RankingManager", daemon=True)
            self.manager_thread.start()

    def stop_manager_thread(self):
        """백그라운드 정리 스레드를 종료"""
        if self.manager_thread and self.manager_thread.is_alive():
            self.stop_event.set()
            self.manager_thread.join(timeout=5)

    def get_final_ranking_board_message(self):
        """프로그램 종료 시 최종 랭킹 보드를 포맷하여 반환"""
        with self.lock:
            low_msg = f"--- 최종 저점 랭킹 ---\n{self._format_board(self.boards['low'], board_type='low')}"
            high_msg = f"--- 최종 고점 랭킹 ---\n{self._format_board(self.boards['high'], board_type='high')}"
            return f"\n{low_msg}\n\n{high_msg}"