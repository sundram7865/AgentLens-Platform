"""The SupportPilot drop-in module must actually work.

`integrations/supportpilot/observability.py` is copy-pasted into another
repository, which means it gets no CI of its own and any breakage surfaces as a
mysterious failure in someone else's project. So it is imported and exercised
here, against the same fake LangChain surface the SDK tests use.
"""

from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from obs_sdk import InMemoryPublisher, Observability, SpanKind
from obs_sdk.schema import EventType

INTEGRATION = Path(__file__).resolve().parents[3] / "integrations/supportpilot/observability.py"


@pytest.fixture
def module(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Import the drop-in file by path, as SupportPilot would after copying it."""
    spec = importlib.util.spec_from_file_location("sp_observability", INTEGRATION)
    assert spec and spec.loader
    loaded = importlib.util.module_from_spec(spec)
    sys.modules["sp_observability"] = loaded
    spec.loader.exec_module(loaded)

    publisher = InMemoryPublisher()
    monkeypatch.setattr(loaded, "_obs", Observability(publisher=publisher, service="supportpilot"))
    loaded.captured = publisher  # type: ignore[attr-defined]
    try:
        yield loaded
    finally:
        sys.modules.pop("sp_observability", None)


class FakeLLMResult:
    def __init__(self) -> None:
        message = type(
            "M",
            (),
            {
                "usage_metadata": {"input_tokens": 900, "output_tokens": 120},
                "response_metadata": {"model_name": "gemini-2.0-flash"},
                "content": "REFUND_REQUEST",
            },
        )()
        generation = type(
            "G", (), {"text": "REFUND_REQUEST", "message": message, "generation_info": {}}
        )()
        self.generations = [[generation]]
        self.llm_output = {}


class TestAgentConfig:
    def test_returns_a_langgraph_config_with_callbacks_and_metadata(self, module) -> None:
        org, ticket = uuid.uuid4(), uuid.uuid4()
        config = module.agent_config(
            organization_id=org,
            ticket_id=ticket,
            agent_run_id=uuid.uuid4(),
            category="REFUND_REQUEST",
        )
        assert len(config["callbacks"]) == 1
        # Both keys matter: callbacks is what LangChain invokes, metadata is what
        # a node's own sub-chains inherit the trace id from.
        assert config["metadata"]["organization_id"] == str(org)
        assert config["metadata"]["tenant_id"] == str(org)
        assert config["metadata"]["ticket_id"] == str(ticket)
        assert config["metadata"]["category"] == "REFUND_REQUEST"
        assert config["metadata"]["trace_id"].startswith("tr_")

    def test_uuid_arguments_are_stringified(self, module) -> None:
        """SQLAlchemy hands these back as UUID objects, not strings."""
        config = module.agent_config(organization_id=uuid.uuid4(), ticket_id=uuid.uuid4())
        assert isinstance(config["metadata"]["tenant_id"], str)

    def test_a_full_agent_run_produces_a_tenant_scoped_trace(self, module) -> None:
        org = uuid.uuid4()
        trace_id = module.new_agent_trace_id()
        config = module.agent_config(organization_id=org, ticket_id=uuid.uuid4(), trace_id=trace_id)
        handler = config["callbacks"][0]

        root, node, llm = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        handler.on_chain_start({"name": "LangGraph"}, {}, run_id=root)
        handler.on_chain_start(
            {"name": "step"},
            {},
            run_id=node,
            parent_run_id=root,
            metadata={"langgraph_node": "classify_ticket_step"},
        )
        handler.on_llm_start(
            {"name": "ChatGoogleGenerativeAI"}, ["classify"], run_id=llm, parent_run_id=node
        )
        handler.on_llm_end(FakeLLMResult(), run_id=llm)
        handler.on_chain_end({}, run_id=node)
        handler.on_chain_end({"decision": "AUTO_REPLY_DRAFT"}, run_id=root)

        events = module.captured.events
        assert {e.tenant_id for e in events} == {str(org)}
        assert {e.trace_id for e in events} == {trace_id}
        assert any(e.name == "classify_ticket_step" for e in events)
        llm_end = next(e for e in events if e.kind is SpanKind.LLM and e.type is EventType.SPAN_END)
        assert llm_end.usage is not None and llm_end.usage.total_tokens == 1020
        assert llm_end.model == "gemini-2.0-flash"

    def test_concurrent_organizations_do_not_braid(self, module) -> None:
        org_a, org_b = uuid.uuid4(), uuid.uuid4()
        handler_a = module.agent_config(organization_id=org_a, ticket_id=uuid.uuid4())["callbacks"][
            0
        ]
        handler_b = module.agent_config(organization_id=org_b, ticket_id=uuid.uuid4())["callbacks"][
            0
        ]

        run_a, run_b = uuid.uuid4(), uuid.uuid4()
        handler_a.on_chain_start({"name": "a"}, {}, run_id=run_a)
        handler_b.on_chain_start({"name": "b"}, {}, run_id=run_b)
        handler_b.on_chain_end({}, run_id=run_b)
        handler_a.on_chain_end({}, run_id=run_a)

        by_tenant: dict[str, set[str]] = {}
        for event in module.captured.events:
            by_tenant.setdefault(event.tenant_id, set()).add(event.trace_id)
        assert len(by_tenant[str(org_a)]) == 1
        assert len(by_tenant[str(org_b)]) == 1
        assert by_tenant[str(org_a)] != by_tenant[str(org_b)]


class TestToolGateway:
    def test_tool_execution_is_recorded_with_its_scope(self, module) -> None:
        org, ticket = uuid.uuid4(), uuid.uuid4()
        with module.observe_tool_execution(
            organization_id=org,
            tool_name="urbankart_request_refund",
            scope="HIGH_RISK_WRITE",
            arguments={"order_id": "A-1", "amount": 4999},
            ticket_id=ticket,
        ) as span:
            span.set_output(status="BLOCKED_APPROVAL_REQUIRED")

        end = next(
            e
            for e in module.captured.events
            if e.type is EventType.SPAN_END and e.name == "urbankart_request_refund"
        )
        assert end.kind is SpanKind.TOOL
        assert end.tenant_id == str(org)
        assert end.attributes["tool_scope"] == "HIGH_RISK_WRITE"
        assert end.attributes["ticket_id"] == str(ticket)
        assert end.output["status"] == "BLOCKED_APPROVAL_REQUIRED"

    def test_a_failing_provider_call_still_produces_a_span(self, module) -> None:
        """The failed refund is the one you need in the dashboard."""
        with (
            pytest.raises(RuntimeError),
            module.observe_tool_execution(
                organization_id=uuid.uuid4(),
                tool_name="urbankart_request_refund",
                scope="HIGH_RISK_WRITE",
            ),
        ):
            raise RuntimeError("provider returned 503")

        end = next(e for e in module.captured.events if e.type is EventType.SPAN_END)
        assert end.status.value == "error"
        assert end.error is not None and "503" in end.error.message


class TestSafety:
    def test_disabled_by_config_is_a_total_no_op(self, monkeypatch) -> None:
        """This is what makes the change safe to merge before deploying."""
        monkeypatch.setenv("OBS_ENABLED", "false")
        monkeypatch.delenv("OBS_REDIS_URL", raising=False)

        spec = importlib.util.spec_from_file_location("sp_observability_off", INTEGRATION)
        assert spec and spec.loader
        loaded = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(loaded)

        config = loaded.agent_config(organization_id="org", ticket_id="T-1")
        handler = config["callbacks"][0]
        run = uuid.uuid4()
        handler.on_chain_start({"name": "x"}, {}, run_id=run)  # must not raise
        handler.on_chain_end({}, run_id=run)

        with loaded.observe_tool_execution(organization_id="org", tool_name="t", scope="READ_ONLY"):
            pass

        loaded.shutdown_observability(timeout=0.1)
        # NullPublisher still reports counters, all zero -- so the host's own
        # /health payload has a stable shape whether or not this is switched on.
        stats = loaded.observability_stats()
        assert stats["published"] == 0
        assert stats["dropped_queue_full"] == 0
        assert stats["connect_failures"] == 0

    def test_stats_are_exposed_for_the_host_health_endpoint(self, module) -> None:
        module.observe_tool_execution(
            organization_id="org", tool_name="t", scope="READ_ONLY"
        ).__enter__()
        assert "published" in module.observability_stats()

    def test_shutdown_is_idempotent(self, module) -> None:
        module.shutdown_observability(timeout=0.1)
        module.shutdown_observability(timeout=0.1)
