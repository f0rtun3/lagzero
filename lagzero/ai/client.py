from __future__ import annotations

from typing import Protocol


class LLMClient(Protocol):
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        """Generate a model response."""


class DisabledLLMClient:
    def generate(self, system_prompt: str, user_prompt: str) -> str:
        raise RuntimeError("AI explanation is disabled.")


class StaticLLMClient:
    def __init__(self, response: str) -> None:
        self._response = response
        self.calls = 0

    def generate(self, system_prompt: str, user_prompt: str) -> str:
        self.calls += 1
        return self._response

