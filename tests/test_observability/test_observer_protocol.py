"""Tests for ExecutionObserverProtocol and NoOpObserver."""
import inspect
import pytest
from orcapod.pipeline.observer import NoOpObserver
from orcapod.protocols.observability_protocols import ExecutionObserverProtocol


class TestNoOpObserver:
    def test_contextualize_returns_observer(self):
        """contextualize() returns an object implementing the protocol."""
        observer = NoOpObserver()
        contextualized = observer.contextualize("pod_uri", "schema_abc", "inst_xyz")
        assert isinstance(contextualized, ExecutionObserverProtocol)

    def test_noop_contextualize_returns_self(self):
        """NoOpObserver.contextualize() returns self (no wrapping needed)."""
        observer = NoOpObserver()
        result = observer.contextualize("a", "b")
        assert result is observer

    def test_contextualize_accepts_variadic_identity_path(self):
        """contextualize() accepts any number of string args."""
        observer = NoOpObserver()
        # Zero args
        result0 = observer.contextualize()
        assert result0 is observer
        # One arg
        result1 = observer.contextualize("pod_uri")
        assert result1 is observer
        # Three args
        result3 = observer.contextualize("pod_uri", "schema_abc", "inst_xyz")
        assert result3 is observer

    def test_noop_hooks_accept_no_pipeline_path(self):
        """Hooks no longer accept pipeline_path parameter."""
        observer = NoOpObserver()
        for method_name in ["on_node_start", "on_node_end", "create_packet_logger"]:
            if hasattr(observer, method_name):
                sig = inspect.signature(getattr(observer, method_name))
                assert "pipeline_path" not in sig.parameters, \
                    f"{method_name} still has pipeline_path parameter"

    def test_protocol_hooks_accept_no_pipeline_path(self):
        """Protocol hook signatures no longer include pipeline_path."""
        for method_name in ["on_node_start", "on_node_end", "create_packet_logger"]:
            if hasattr(ExecutionObserverProtocol, method_name):
                sig = inspect.signature(getattr(ExecutionObserverProtocol, method_name))
                assert "pipeline_path" not in sig.parameters, \
                    f"Protocol {method_name} still has pipeline_path parameter"

    def test_protocol_contextualize_uses_variadic_identity_path(self):
        """Protocol contextualize() uses *identity_path: str, not fixed params."""
        sig = inspect.signature(ExecutionObserverProtocol.contextualize)
        params = list(sig.parameters.values())
        # Find a VAR_POSITIONAL param named identity_path
        var_positional = [p for p in params if p.kind == inspect.Parameter.VAR_POSITIONAL]
        assert len(var_positional) == 1, \
            "contextualize() should have exactly one *args (VAR_POSITIONAL) parameter"
        assert var_positional[0].name == "identity_path"

    def test_noop_on_node_start_callable_without_pipeline_path(self):
        """on_node_start runs without error on NoOpObserver (no pipeline_path)."""
        observer = NoOpObserver()
        # Should work without pipeline_path
        observer.on_node_start(node_label="test_node", node_hash="abc123")

    def test_noop_on_node_end_callable_without_pipeline_path(self):
        """on_node_end runs without error on NoOpObserver (no pipeline_path)."""
        observer = NoOpObserver()
        observer.on_node_end(node_label="test_node", node_hash="abc123")

    def test_noop_satisfies_protocol(self):
        """NoOpObserver instance satisfies ExecutionObserverProtocol runtime check."""
        observer = NoOpObserver()
        assert isinstance(observer, ExecutionObserverProtocol)
