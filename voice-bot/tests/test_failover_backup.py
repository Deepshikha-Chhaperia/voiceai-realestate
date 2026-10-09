"""Backup-model failover: Groq primary fails (429 / timeout) -> second Groq model -> recovery line.

No network. The Groq HTTP client is replaced by a fake; the real GroqLLMService and the real
_attach_resilient_failover wrapper run unchanged.
"""
import asyncio
import os
from types import SimpleNamespace

import pytest
import yaml

os.environ.setdefault("GROQ_API_KEY", "test-key-not-real")

import bot  # noqa: E402
from pipecat.processors.aggregators.llm_context import LLMContext  # noqa: E402

HERE = os.path.dirname(os.path.dirname(__file__))


def _config():
    with open(os.path.join(HERE, "config.yaml"), encoding="utf-8") as f:
        base = yaml.safe_load(f)
    with open(os.path.join(HERE, "profiles", "india.yaml"), encoding="utf-8") as f:
        prof = yaml.safe_load(f)
    return {**base, **prof}


def _chunk(text, finish=None):
    return SimpleNamespace(
        id="c", object="chat.completion.chunk", created=0, model="m", usage=None,
        choices=[SimpleNamespace(index=0, finish_reason=finish,
                                 delta=SimpleNamespace(content=text, role="assistant", tool_calls=None))],
    )


class _Stream:
    def __init__(self, texts):
        self._chunks = [_chunk(t) for t in texts]

    def __aiter__(self):
        async def gen():
            for c in self._chunks:
                yield c
        return gen()


class _RateLimit(Exception):
    status_code = 429

    def __init__(self):
        super().__init__("Error code: 429 - rate_limit_exceeded")


def _make(monkeypatch, primary_behavior, backup_behavior):
    cfg = _config()
    cfg["llm_backup_groq_model"] = "llama-3.3-70b-versatile"  # fake, explicitly enabled fixture
    service = bot.ServiceFactory.create("llm", "groq", cfg)
    calls = []

    async def fake_create(**params):
        calls.append(params)
        behavior = primary_behavior if params["model"] == cfg["providers"]["llm"]["groq"]["params"]["model"] else backup_behavior
        if behavior == "429":
            raise _RateLimit()
        if behavior == "hang":
            await asyncio.sleep(30)
        return _Stream(["Sure, ", "happy to help."] if behavior == "ok" else ["x"])

    service._client.chat.completions.create = fake_create
    return service, calls, cfg


async def _collect(service):
    ctx = LLMContext([{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}])
    stream = await service.get_chat_completions(ctx)
    out = []
    async for c in stream:
        if c.choices and c.choices[0].delta.content:
            out.append(c.choices[0].delta.content)
    return "".join(out)


async def test_primary_ok_never_touches_backup(monkeypatch):
    service, calls, cfg = _make(monkeypatch, "ok", "ok")
    assert await _collect(service) == "Sure, happy to help."
    assert [c["model"] for c in calls] == [cfg["providers"]["llm"]["groq"]["params"]["model"]]


async def test_429_falls_back_to_second_groq_model(monkeypatch):
    service, calls, cfg = _make(monkeypatch, "429", "ok")
    text = await _collect(service)
    assert text == "Sure, happy to help."
    models = [c["model"] for c in calls]
    assert models == [cfg["providers"]["llm"]["groq"]["params"]["model"], "llama-3.3-70b-versatile"]
    backup = calls[1]
    assert "reasoning_effort" not in backup          # Llama rejects it
    assert "service_tier" not in backup
    assert backup["stream"] is True
    assert backup["messages"] and backup["messages"][-1]["content"] == "hi"


async def test_circuit_breaker_sends_next_turn_straight_to_backup(monkeypatch):
    service, calls, cfg = _make(monkeypatch, "429", "ok")
    await _collect(service)
    calls.clear()
    assert await _collect(service) == "Sure, happy to help."
    assert [c["model"] for c in calls] == ["llama-3.3-70b-versatile"]


async def test_primary_timeout_falls_back(monkeypatch):
    service, calls, cfg = _make(monkeypatch, "hang", "ok")
    assert await _collect(service) == "Sure, happy to help."
    assert calls[-1]["model"] == "llama-3.3-70b-versatile"


async def test_both_fail_speaks_recovery_line_not_crash(monkeypatch):
    service, calls, cfg = _make(monkeypatch, "429", "429")
    text = await _collect(service)
    assert text  # deterministic recovery line, call survives
    assert len(calls) == 2


async def test_backup_can_be_disabled_by_empty_config(monkeypatch):
    cfg = _config()
    cfg["llm_backup_groq_model"] = ""
    service = bot.ServiceFactory.create("llm", "groq", cfg)
    calls = []

    async def fake_create(**params):
        calls.append(params["model"])
        raise _RateLimit()

    service._client.chat.completions.create = fake_create
    assert await _collect(service)
    assert calls == [cfg["providers"]["llm"]["groq"]["params"]["model"]]

async def test_exhausted_providers_use_existing_goodbye_callback(monkeypatch):
    from unittest.mock import AsyncMock
    service, calls, cfg = _make(monkeypatch, '429', '429')
    service._on_provider_exhausted = AsyncMock()
    await _collect(service)
    service._on_provider_exhausted.assert_not_awaited()
    text = await _collect(service)
    assert 'try this call again later' in text
    service._on_provider_exhausted.assert_awaited_once()
    assert await _collect(service) == ''
    service._on_provider_exhausted.assert_awaited_once()
