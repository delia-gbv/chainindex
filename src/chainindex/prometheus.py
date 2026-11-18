"""Prometheus metrics declared as class attributes and built once per instance.

    class Metrics(MetricsRegistry):
        polls = counter("polls_total", "Polls completed", labels=["lane"])

    metrics = Metrics("chainindex")
    metrics.polls.labels(lane="tip").inc()
    metrics.serve(9100)

Names are prefixed with ``<namespace>_``. A registry made with ``isolated=True`` (tests) has its own
``CollectorRegistry``, so it can be built again and again.
"""

from collections.abc import Sequence
from typing import Any

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram, start_http_server
from prometheus_client.metrics import MetricWrapperBase
from pydantic import BaseModel, ConfigDict


class MetricSpec(BaseModel):
    """Describes one metric that a registry builds when it is instantiated."""

    model_config = ConfigDict(frozen=True)

    kind: type[MetricWrapperBase]
    name: str
    documentation: str
    labels: tuple[str, ...] = ()
    options: dict[str, Any] = {}

    def build(self, namespace: str, registry: CollectorRegistry) -> MetricWrapperBase:
        """Creates the metric in a registry."""
        return self.kind(
            self.name, self.documentation, labelnames=self.labels, namespace=namespace, registry=registry, **self.options
        )


def counter(name: str, documentation: str, *, labels: Sequence[str] = ()) -> Any:
    """Declares a counter."""
    return MetricSpec(kind=Counter, name=name, documentation=documentation, labels=tuple(labels))


def gauge(name: str, documentation: str, *, labels: Sequence[str] = ()) -> Any:
    """Declares a gauge."""
    return MetricSpec(kind=Gauge, name=name, documentation=documentation, labels=tuple(labels))


def histogram(name: str, documentation: str, *, labels: Sequence[str] = (), buckets: Sequence[float] | None = None) -> Any:
    """Declares a histogram, with its own buckets when given."""
    options = {"buckets": tuple(buckets)} if buckets is not None else {}
    return MetricSpec(kind=Histogram, name=name, documentation=documentation, labels=tuple(labels), options=options)


class MetricsRegistry:
    """Turns every ``MetricSpec`` class attribute into a real metric on the instance."""

    def __init__(self, namespace: str, *, isolated: bool = False):
        self.registry = CollectorRegistry() if isolated else REGISTRY
        specs: dict[str, MetricSpec] = {}
        for base in reversed(type(self).__mro__):
            specs.update({name: value for name, value in vars(base).items() if isinstance(value, MetricSpec)})
        for name, spec in specs.items():
            setattr(self, name, spec.build(namespace, self.registry))

    def serve(self, port: int | None) -> None:
        """Starts the scrape endpoint on ``/metrics`` when ``port`` is set."""
        if port is not None:
            start_http_server(port, addr="0.0.0.0", registry=self.registry)
