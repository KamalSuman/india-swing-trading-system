import enum
import hashlib
import os
import re
from dataclasses import dataclass
from typing import Callable, Protocol, Tuple, Optional
from datetime import datetime

# The module must not modify interpreter-global import state, so we import directly.
# We assume the test framework or caller has configured the module lookup path appropriately.
from worker_status import (
    WorkerPhase, WorkerReason, WorkerStatus,
    write_status_atomic, read_status, validate_transition
)

class OneShotRunnerError(ValueError):
    """Static sanitized error for runner operations."""
    pass

class AttemptOutcome(enum.Enum):
    HANDOFF_READY = "HANDOFF_READY"
    SESSION_LIMIT = "SESSION_LIMIT"
    RATE_LIMIT = "RATE_LIMIT"
    TIMEOUT = "TIMEOUT"
    LAUNCH_ERROR = "LAUNCH_ERROR"
    PROCESS_EXIT_NONZERO = "PROCESS_EXIT_NONZERO"
    PRIOR_ATTEMPT_NO_HANDOFF = "PRIOR_ATTEMPT_NO_HANDOFF"

O_BINARY_FLAG = getattr(os, 'O_BINARY', 0)

def _canonical_path(p: str) -> str:
    return os.path.normcase(os.path.realpath(os.path.abspath(p)))

@dataclass(frozen=True, slots=True)
class OneShotRequest:
    task_id: str
    task_revision: int
    task_token: str
    task_bytes: bytes
    expected_sha256: str
    project_root: str
    runtime_root: str
    timeout_seconds: int
    claude_executable: str
    allowed_tools: Tuple[str, ...]

    def __post_init__(self):
        if not isinstance(self.task_id, str) or not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{0,127}", self.task_id):
            raise OneShotRunnerError("invalid task_id format")
            
        if type(self.task_revision) is not int or self.task_revision <= 0:
            raise OneShotRunnerError("invalid task_revision")
            
        if not isinstance(self.task_token, str) or not re.fullmatch(r"[0-9a-f]{64}", self.task_token):
            raise OneShotRunnerError("invalid task_token format")
            
        expected_token_src = f"{self.task_id}\n{self.task_revision}".encode('utf-8')
        if hashlib.sha256(expected_token_src).hexdigest() != self.task_token:
            raise OneShotRunnerError("task_token mismatch")

        if not isinstance(self.task_bytes, bytes) or len(self.task_bytes) == 0 or len(self.task_bytes) > 262144:
            raise OneShotRunnerError("invalid task_bytes length")
            
        if not isinstance(self.expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", self.expected_sha256):
            raise OneShotRunnerError("invalid expected_sha256 format")
            
        if type(self.timeout_seconds) is not int or self.timeout_seconds < 1 or self.timeout_seconds > 3600:
            raise OneShotRunnerError("invalid timeout_seconds")
            
        if not isinstance(self.project_root, str) or not os.path.isabs(self.project_root) or not os.path.isdir(self.project_root):
            raise OneShotRunnerError("invalid project_root")
            
        if not isinstance(self.runtime_root, str) or not os.path.isabs(self.runtime_root):
            raise OneShotRunnerError("invalid runtime_root")
            
        expected_runtime = _canonical_path(os.path.join(self.project_root, 'agent-control', 'dispatcher', 'logs', 'one-shot'))
        if _canonical_path(self.runtime_root) != expected_runtime:
            raise OneShotRunnerError("runtime_root must match canonical expected path")
            
        if (
            not isinstance(self.claude_executable, str)
            or len(self.claude_executable) == 0
            or self.claude_executable != self.claude_executable.strip()
        ):
            raise OneShotRunnerError("invalid claude_executable")
            
        if re.search(r'[\x00-\x1f\x7f]', self.claude_executable):
            raise OneShotRunnerError("claude_executable contains control characters")
            
        if self.allowed_tools != ('Bash(rtk:*)',):
            raise OneShotRunnerError("allowed_tools immutable violation")

class ProcessHandle(Protocol):
    @property
    def pid(self) -> int: ...
    def wait(self, timeout_seconds: int) -> int: ...
    def terminate_tree(self) -> None: ...

class ProcessLauncher(Protocol):
    def launch(self, argv: Tuple[str, ...], cwd: str, transcript_path: str) -> ProcessHandle: ...

class MatchingHandoffChecker(Protocol):
    def has_matching_handoff(self, task_id: str, task_revision: int) -> bool: ...

def run_one_shot(
    request: OneShotRequest,
    launcher: ProcessLauncher,
    handoff_checker: MatchingHandoffChecker,
    now_utc: Callable[[], datetime]
) -> AttemptOutcome:
    
    rt_root = request.runtime_root
    claim_path = os.path.join(rt_root, "claims", f"{request.task_token}.claim")
    snapshot_path = os.path.join(rt_root, "snapshots", f"{request.expected_sha256}.json")
    status_path = os.path.join(rt_root, "status", f"{request.task_token}.json")
    transcript_path = os.path.join(rt_root, "transcripts", f"{request.task_token}.log")

    # 1. State machine - pre-existing status checks
    try:
        status_exists = os.path.exists(status_path)
    except Exception:
        raise OneShotRunnerError("status check failed")
    if status_exists:
        raise OneShotRunnerError("pre-existing status blocks execution")
        
    try:
        claim_exists = os.path.exists(claim_path)
    except Exception:
        raise OneShotRunnerError("claim check failed")
    if claim_exists:
        raise OneShotRunnerError("pre-existing claim blocks execution")

    # 2. Durable Exclusive Claim
    try:
        os.makedirs(os.path.dirname(claim_path), exist_ok=True)
        fd = os.open(claim_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_BINARY_FLAG)
        with os.fdopen(fd, 'wb') as f:
            f.write(b"claimed\n")
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        raise OneShotRunnerError("claim creation failed")

    # 3. Snapshot verification and writing
    if hashlib.sha256(request.task_bytes).hexdigest() != request.expected_sha256:
        raise OneShotRunnerError("task_bytes hash mismatch before launch")

    try:
        os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
    except Exception:
        raise OneShotRunnerError("snapshot directory creation failed")

    try:
        fd_snap = os.open(snapshot_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_BINARY_FLAG)
        with os.fdopen(fd_snap, 'wb') as f:
            f.write(request.task_bytes)
            f.flush()
            os.fsync(f.fileno())
    except FileExistsError:
        try:
            with open(snapshot_path, 'rb') as f:
                existing_bytes = f.read(262145)
        except Exception:
            raise OneShotRunnerError("snapshot read failed")
            
        if existing_bytes != request.task_bytes:
            raise OneShotRunnerError("existing snapshot mismatch")
    except Exception:
        raise OneShotRunnerError("snapshot write failed")

    def _read_verified_snapshot() -> bytes:
        try:
            with open(snapshot_path, 'rb') as f:
                snapshot_bytes = f.read(262145)
        except Exception:
            raise OneShotRunnerError("snapshot verification read failed")
        if snapshot_bytes != request.task_bytes:
            raise OneShotRunnerError("snapshot verification failed")
        if hashlib.sha256(snapshot_bytes).hexdigest() != request.expected_sha256:
            raise OneShotRunnerError("snapshot verification failed")
        return snapshot_bytes

    # Close the write-to-launch window: Claude is never launched until the
    # exact content-addressed snapshot has been read back and re-verified.
    _read_verified_snapshot()

    def _safe_now() -> datetime:
        try:
            return now_utc()
        except Exception:
            raise OneShotRunnerError("clock failed")

    def _safe_write_status(status_obj: WorkerStatus) -> None:
        try:
            if os.path.exists(status_path):
                prev = read_status(status_path)
                validate_transition(prev, status_obj)
            write_status_atomic(status_path, status_obj)
        except Exception:
            raise OneShotRunnerError("status write failed")

    def _safe_terminate(handle: ProcessHandle) -> None:
        try:
            term_func = handle.terminate_tree
        except Exception:
            raise OneShotRunnerError("terminate property read failed")
        
        if not callable(term_func):
            raise OneShotRunnerError("terminate is not callable")

        try:
            term_func()
        except Exception:
            raise OneShotRunnerError("terminate call failed")

    def _safe_fallback_terminal(pid_val: Optional[int]) -> None:
        """Write a fallback TERMINAL PRIOR_ATTEMPT_NO_HANDOFF without crashing."""
        try:
            ts = _safe_now()
            ws_term = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF, ts, pid_val, None)
            _safe_write_status(ws_term)
        except Exception:
            raise OneShotRunnerError("fallback terminal write failed")

    # Launch configuration
    prompt = (
        "Only the immutable snapshot is authoritative. "
        f"Snapshot path: {snapshot_path}\n"
        f"Expected hash: {request.expected_sha256}\n"
        "You must verify the hash before acting. "
        "Write the exact outbox. Stop after handoff."
    )
    
    argv = (
        request.claude_executable.strip(),
        '-p', prompt,
        '--permission-mode', 'acceptEdits',
        '--allowedTools', request.allowed_tools[0]
    )

    try:
        os.makedirs(os.path.dirname(transcript_path), exist_ok=True)
        os.makedirs(os.path.dirname(status_path), exist_ok=True)
    except Exception:
        raise OneShotRunnerError("runtime directory creation failed")

    # Launch
    try:
        handle = launcher.launch(argv=argv, cwd=request.project_root, transcript_path=transcript_path)
    except Exception:
        try:
            ts = _safe_now()
            ws = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, WorkerReason.LAUNCH_ERROR, ts, None, None)
        except Exception:
            raise OneShotRunnerError("status creation failed")
        _safe_write_status(ws)
        return AttemptOutcome.LAUNCH_ERROR

    # Read PID safely
    try:
        pid = getattr(handle, "pid", None)
    except Exception:
        pid = None

    if type(pid) is not int or pid <= 0 or type(pid) is bool:
        try:
            _safe_terminate(handle)
        except Exception:
            raise OneShotRunnerError("invalid handle termination failed")
        
        try:
            ts = _safe_now()
            ws = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, WorkerReason.LAUNCH_ERROR, ts, None, None)
        except Exception:
            raise OneShotRunnerError("status creation failed")
        _safe_write_status(ws)
        return AttemptOutcome.LAUNCH_ERROR

    # Running Phase
    try:
        ts = _safe_now()
        ws_run = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.RUNNING, WorkerReason.NONE, ts, pid, None)
    except Exception:
        try:
            _safe_terminate(handle)
        except Exception:
            raise OneShotRunnerError("running status creation and termination failed")
        raise OneShotRunnerError("running status creation failed")

    try:
        _safe_write_status(ws_run)
    except Exception:
        try:
            _safe_terminate(handle)
        except Exception:
            raise OneShotRunnerError("running status write and termination failed")
        raise OneShotRunnerError("running status write failed")

    # Wait
    exit_code = None
    timeout_occurred = False
    try:
        exit_code = handle.wait(request.timeout_seconds)
    except TimeoutError:
        timeout_occurred = True
        try:
            _safe_terminate(handle)
        except Exception:
            raise OneShotRunnerError("timeout termination failed")
    except Exception:
        try:
            _safe_terminate(handle)
        except Exception:
            raise OneShotRunnerError("wait failed and termination failed")
        raise OneShotRunnerError("wait failed")

    # Post-launch Re-verify Snapshot
    try:
        _read_verified_snapshot()
    except OneShotRunnerError:
        _safe_fallback_terminal(pid)
        return AttemptOutcome.PRIOR_ATTEMPT_NO_HANDOFF

    # Check Handoff first (always wins)
    try:
        has_handoff = handoff_checker.has_matching_handoff(request.task_id, request.task_revision)
        if type(has_handoff) is not bool:
            raise OneShotRunnerError("handoff_checker returned non-bool")
    except Exception:
        # Handoff checker failed. If we hit this, we write fallback terminal and raise fixed error.
        _safe_fallback_terminal(pid)
        raise OneShotRunnerError("handoff check failed")

    if has_handoff:
        try:
            ts = _safe_now()
            ws_handoff = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, ts, pid, None)
        except Exception:
            raise OneShotRunnerError("handoff status creation failed")
        _safe_write_status(ws_handoff)
        return AttemptOutcome.HANDOFF_READY

    # No Handoff. 
    # Valid exit_code type verification
    if not timeout_occurred:
        if type(exit_code) is not int or type(exit_code) is bool:
            _safe_fallback_terminal(pid)
            raise OneShotRunnerError("invalid exit_code from handle")

    if timeout_occurred:
        try:
            ts = _safe_now()
            ws_timeout = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, WorkerReason.TIMEOUT, ts, pid, None)
        except Exception:
            raise OneShotRunnerError("timeout status creation failed")
        _safe_write_status(ws_timeout)
        return AttemptOutcome.TIMEOUT

    # Completed Wait, no handoff, check transcript
    try:
        with open(transcript_path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            read_size = min(65536, size)
            if read_size > 0:
                f.seek(size - read_size, os.SEEK_SET)
                transcript_tail = f.read(read_size).decode('utf-8', errors='replace').lower()
            else:
                transcript_tail = ""
    except Exception:
        _safe_fallback_terminal(pid)
        raise OneShotRunnerError("transcript read failed")

    # Classify Transcript
    reason = None
    if "session limit" in transcript_tail or "usage limit" in transcript_tail:
        reason = WorkerReason.SESSION_LIMIT
    elif "rate limit" in transcript_tail or "too many requests" in transcript_tail:
        reason = WorkerReason.RATE_LIMIT

    if reason is not None:
        try:
            ts = _safe_now()
            ws_term = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, reason, ts, pid, None)
        except Exception:
            raise OneShotRunnerError("quota status creation failed")
        _safe_write_status(ws_term)
        return AttemptOutcome[reason.name]

    if exit_code != 0:
        try:
            ts = _safe_now()
            ws_term = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, WorkerReason.PROCESS_EXIT_NONZERO, ts, pid, exit_code)
        except Exception:
            raise OneShotRunnerError("nonzero exit status creation failed")
        _safe_write_status(ws_term)
        return AttemptOutcome.PROCESS_EXIT_NONZERO

    # Zero exit, no handoff
    try:
        ts = _safe_now()
        ws_term = WorkerStatus(request.task_token, request.task_revision, WorkerPhase.TERMINAL, WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF, ts, pid, None)
    except Exception:
        raise OneShotRunnerError("no handoff status creation failed")
    _safe_write_status(ws_term)
    return AttemptOutcome.PRIOR_ATTEMPT_NO_HANDOFF
