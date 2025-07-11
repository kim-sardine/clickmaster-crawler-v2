from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional


class BatchStatus(Enum):
    """배치 작업 상태"""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchProgress:
    """배치 진행률 정보"""

    total_requests: int
    completed_requests: int
    failed_requests: int

    @property
    def completion_rate(self) -> float:
        """완료율 계산"""
        if self.total_requests == 0:
            return 0.0
        return (self.completed_requests / self.total_requests) * 100


@dataclass
class BatchSummary:
    """배치 작업 요약 정보"""

    batch_id: str
    status: BatchStatus
    progress: BatchProgress
    created_at: str
    completed_at: Optional[str] = None
    error_count: int = 0

    def is_ready_for_processing(self) -> bool:
        """결과 처리 준비 상태 확인"""
        return self.status == BatchStatus.COMPLETED and self.progress.failed_requests == 0
