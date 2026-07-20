import enum
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

class StatusError(ValueError):
    """Static sanitized error for status operations."""
    pass

class WorkerPhase(enum.Enum):
    RUNNING = "RUNNING"
    HANDOFF_READY = "HANDOFF_READY"
    TERMINAL = "TERMINAL"

class WorkerReason(enum.Enum):
    NONE = "NONE"
    SUCCESS = "SUCCESS"
    SESSION_LIMIT = "SESSION_LIMIT"
    RATE_LIMIT = "RATE_LIMIT"
    TIMEOUT = "TIMEOUT"
    LAUNCH_ERROR = "LAUNCH_ERROR"
    PROCESS_EXIT_NONZERO = "PROCESS_EXIT_NONZERO"
    STALE_RUNNING_NO_PROCESS = "STALE_RUNNING_NO_PROCESS"
    PRIOR_ATTEMPT_NO_HANDOFF = "PRIOR_ATTEMPT_NO_HANDOFF"

@dataclass(frozen=True, slots=True)
class WorkerStatus:
    task_token: str
    task_revision: int
    phase: WorkerPhase
    reason: WorkerReason
    observed_at_utc: datetime
    pid: Optional[int]
    exit_code: Optional[int]

    def __post_init__(self):
        if not isinstance(self.task_token, str) or not re.fullmatch(r"[0-9a-f]{64}", self.task_token):
            raise StatusError("invalid task_token format")
            
        if type(self.task_revision) is not int or self.task_revision <= 0:
            raise StatusError("invalid task_revision")
            
        if not isinstance(self.phase, WorkerPhase):
            raise StatusError("invalid phase")
            
        if not isinstance(self.reason, WorkerReason):
            raise StatusError("invalid reason")
            
        if not isinstance(self.observed_at_utc, datetime):
            raise StatusError("invalid observed_at_utc type")
            
        try:
            if self.observed_at_utc.tzinfo is None:
                raise StatusError("observed_at_utc must be timezone-aware UTC")
            offset = self.observed_at_utc.tzinfo.utcoffset(self.observed_at_utc)
            if offset is None or offset.total_seconds() != 0:
                raise StatusError("observed_at_utc must be timezone-aware UTC")
        except StatusError:
            raise
        except Exception:
            raise StatusError("invalid tzinfo behavior")
            
        if self.pid is not None:
            if type(self.pid) is not int or self.pid <= 0:
                raise StatusError("invalid pid")
                
        if self.exit_code is not None:
            if type(self.exit_code) is not int:
                raise StatusError("invalid exit_code")

        if self.phase in (WorkerPhase.RUNNING, WorkerPhase.HANDOFF_READY):
            if self.reason != WorkerReason.NONE:
                raise StatusError("reason must be NONE for active phase")
            if self.pid is None:
                raise StatusError("pid required for active phase")
            if self.exit_code is not None:
                raise StatusError("exit_code must be null for active phase")
        else:
            if self.reason == WorkerReason.NONE:
                raise StatusError("reason required for TERMINAL phase")
                
            if self.reason == WorkerReason.LAUNCH_ERROR:
                if self.pid is not None or self.exit_code is not None:
                    raise StatusError("LAUNCH_ERROR requires null pid and null exit_code")
            elif self.reason in (WorkerReason.SESSION_LIMIT, WorkerReason.RATE_LIMIT, WorkerReason.TIMEOUT, 
                                 WorkerReason.STALE_RUNNING_NO_PROCESS, WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF):
                if self.exit_code is not None:
                    raise StatusError("terminal reason requires null exit_code")
            elif self.reason == WorkerReason.PROCESS_EXIT_NONZERO:
                if self.exit_code is None or self.exit_code == 0 or type(self.exit_code) is bool:
                    raise StatusError("PROCESS_EXIT_NONZERO requires non-zero exit_code")
            elif self.reason == WorkerReason.SUCCESS:
                if self.exit_code != 0 or type(self.exit_code) is bool:
                    raise StatusError("SUCCESS requires exit_code 0")

def serialize_status(status: WorkerStatus) -> bytes:
    if not isinstance(status, WorkerStatus):
        raise StatusError("input must be WorkerStatus")

    try:
        observed_str = status.observed_at_utc.isoformat()
        if observed_str.endswith("+00:00"):
            observed_str = observed_str.replace("+00:00", "Z")
        elif not observed_str.endswith("Z"):
            raise StatusError("observed_at_utc serialization failed")
    except StatusError:
        raise
    except Exception:
        raise StatusError("observed_at_utc serialization failed")
        
    data = {
        "schema_version": 1,
        "task_token": status.task_token,
        "task_revision": status.task_revision,
        "phase": status.phase.value,
        "reason": status.reason.value,
        "observed_at_utc": observed_str,
        "pid": status.pid,
        "exit_code": status.exit_code
    }
    try:
        return json.dumps(data, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode('utf-8')
    except Exception:
        raise StatusError("serialization failed")

def parse_status(content_bytes: bytes) -> WorkerStatus:
    if not isinstance(content_bytes, bytes):
        raise StatusError("input must be bytes")
        
    if len(content_bytes) > 16384:
        raise StatusError("payload too large")
        
    def reject_duplicates(pairs):
        d = {}
        for k, v in pairs:
            if k in d:
                raise StatusError("duplicate keys")
            d[k] = v
        return d

    def reject_floats(val):
        raise StatusError("floats not allowed")

    def reject_constants(val):
        raise StatusError("constants not allowed")

    try:
        content_str = content_bytes.decode('utf-8', errors='strict')
        data = json.loads(
            content_str, 
            object_pairs_hook=reject_duplicates, 
            parse_float=reject_floats,
            parse_constant=reject_constants,
        )
    except Exception:
        raise StatusError("parse failed")
        
    if not isinstance(data, dict):
        raise StatusError("payload must be object")
        
    exact_keys = {"schema_version", "task_token", "task_revision", "phase", "reason", "observed_at_utc", "pid", "exit_code"}
    if set(data.keys()) != exact_keys:
        raise StatusError("exact key set mismatch")
        
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise StatusError("invalid schema_version")
        
    try:
        phase = WorkerPhase(data["phase"])
        reason = WorkerReason(data["reason"])
    except Exception:
        raise StatusError("invalid enum value")
        
    observed_str = data["observed_at_utc"]
    if type(observed_str) is not str or not observed_str.endswith("Z"):
        raise StatusError("invalid observed_at_utc format")
        
    try:
        observed_at_utc = datetime.fromisoformat(observed_str[:-1] + "+00:00")
    except Exception:
        raise StatusError("invalid observed_at_utc format")

    try:
        canonical_str = observed_at_utc.isoformat()
        if canonical_str.endswith("+00:00"):
            canonical_str = canonical_str.replace("+00:00", "Z")
    except Exception:
        raise StatusError("invalid datetime object")
        
    if canonical_str != observed_str:
        raise StatusError("non-canonical observed_at_utc format")
        
    try:
        return WorkerStatus(
            task_token=data["task_token"],
            task_revision=data["task_revision"],
            phase=phase,
            reason=reason,
            observed_at_utc=observed_at_utc,
            pid=data["pid"],
            exit_code=data["exit_code"]
        )
    except StatusError:
        raise
    except Exception:
        raise StatusError("instantiation failed")

def write_status_atomic(path: str, status: WorkerStatus) -> None:
    try:
        data_bytes = serialize_status(status)
        dir_name = os.path.dirname(os.path.abspath(path))
        os.makedirs(dir_name, exist_ok=True)
        
        fd, tmp_path = tempfile.mkstemp(dir=dir_name)
    except Exception:
        raise StatusError("write setup failed")
        
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise StatusError("atomic write failed")

def read_status(path: str) -> WorkerStatus:
    try:
        with open(path, 'rb') as f:
            content_bytes = f.read(16385)
    except Exception:
        raise StatusError("file read failed")
        
    return parse_status(content_bytes)

def validate_transition(previous: WorkerStatus, current: WorkerStatus) -> None:
    if not isinstance(previous, WorkerStatus) or not isinstance(current, WorkerStatus):
        raise StatusError("inputs must be WorkerStatus")

    if previous.task_token != current.task_token or previous.task_revision != current.task_revision:
        raise StatusError("identity mismatch")
        
    if previous == current:
        return
        
    if previous.phase == current.phase:
        raise StatusError("invalid transition: changed data within same phase")
        
    if previous.phase == WorkerPhase.TERMINAL:
        raise StatusError("cannot transition from TERMINAL")
        
    if previous.phase == WorkerPhase.RUNNING:
        if current.phase not in (WorkerPhase.HANDOFF_READY, WorkerPhase.TERMINAL):
            raise StatusError("invalid transition from RUNNING")
            
    if previous.phase == WorkerPhase.HANDOFF_READY:
        if current.phase != WorkerPhase.TERMINAL:
            raise StatusError("invalid transition from HANDOFF_READY")
