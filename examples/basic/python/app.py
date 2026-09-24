"""Send one illustrative LLM workflow trace to a locally running gateway."""
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

provider = TracerProvider(resource=Resource.create({"service.name": "basic-rag-example"}))
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint="http://127.0.0.1:4318/v1/traces")))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("agent.invoke"):
    with tracer.start_as_current_span("retrieval"):
        pass
    with tracer.start_as_current_span("llm.generate") as span:
        # Do not record prompt/completion content until a storage policy exists.
        span.set_attribute("gen_ai.operation.name", "chat")
        span.set_attribute("gen_ai.request.model", "example-model")

provider.force_flush()
print("Trace exported. POST http://127.0.0.1:8000/api/v1/materialize, then open http://127.0.0.1:3000.")
