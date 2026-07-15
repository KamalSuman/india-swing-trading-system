from __future__ import annotations

from typing import Protocol

from india_swing.domain.models import Candidate, DataSnapshot, ResearchAssessment


class ResearchProvider(Protocol):
    """Boundary implemented later by the isolated TradingAgents adapter."""

    model_version: str

    def assess(self, candidate: Candidate, snapshot: DataSnapshot) -> ResearchAssessment: ...
