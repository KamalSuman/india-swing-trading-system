"""Shared operation-scoped exact-ID replay deduplication.

Generalizes the mechanism first built for the promoted-graph publisher so
every promoted store composition (the graph publisher, the engine runner,
and the research bridge that composes both) can deduplicate exact-ID
resolver reads within one top-level operation without ever caching across
separate operations or a process restart.
"""

from __future__ import annotations

from contextlib import contextmanager
from threading import local
from typing import Iterator


class ExactReplayScope:
    """Deduplicate exact immutable resolver reads within one top-level replay.

    The cache is deliberately empty outside ``open()`` and is discarded when
    the outermost operation finishes. A later publication, engine run,
    combined-manifest verification, or process restart therefore performs a
    fresh durable replay and cannot inherit trust from an earlier operation.
    Thread-isolated (backed by ``threading.local``) and nested-scope aware:
    an inner ``open()`` reuses the outer scope's cache rather than starting a
    new one, and only the outermost ``open()`` clears the cache on exit.
    """

    def __init__(self) -> None:
        self._local = local()

    @contextmanager
    def open(self) -> Iterator[None]:
        values = getattr(self._local, "values", None)
        outermost = values is None
        if outermost:
            self._local.values = {}
        try:
            yield
        finally:
            if outermost:
                del self._local.values

    def resolve(self, namespace: str, resolver: object, identity: str) -> object:
        values = getattr(self._local, "values", None)
        if values is None:
            return resolver.get(identity)  # type: ignore[attr-defined]
        key = (namespace, identity)
        if key not in values:
            values[key] = resolver.get(identity)  # type: ignore[attr-defined]
        return values[key]


class ScopedExactResolver:
    """Exact-ID resolver facade backed by one operation-scoped replay cache."""

    def __init__(
        self,
        namespace: str,
        resolver: object,
        replay_scope: ExactReplayScope,
    ) -> None:
        self.namespace = namespace
        self.resolver = resolver
        self.replay_scope = replay_scope

    def get(self, identity: str) -> object:
        return self.replay_scope.resolve(self.namespace, self.resolver, identity)
