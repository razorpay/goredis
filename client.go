package otredis

import (
	"context"

	"github.com/go-redis/redis/extra/rediscmd"
	"github.com/go-redis/redis/v8"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"go.opentelemetry.io/otel/semconv"
)

type Config struct {
	Host     string
	Port     uint16
	Database int
}

type opentracingHook struct {
	tracer opentracing.Tracer
	config Config
}

var _ redis.Hook = &opentracingHook{}

func (h *opentracingHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	span, _ := opentracing.StartSpanFromContextWithTracer(ctx, h.tracer, cmd.FullName())

	ext.SpanKindRPCClient.Set(span)

	// to maintain compatibility with opentelemetry convention
	span.SetTag(string(semconv.DBSystemRedis.Key), semconv.DBSystemRedis.Value.AsString())
	span.SetTag(string(semconv.DBRedisDBIndexKey), h.config.Database)
	span.SetTag(string(semconv.NetTransportTCP.Key), semconv.NetTransportTCP.Value.AsString())
	span.SetTag(string(semconv.DBOperationKey), cmd.FullName())
	span.SetTag(string(semconv.NetPeerNameKey), h.config.Host)
	span.SetTag(string(semconv.NetPeerPortKey), h.config.Port)
	span.SetTag(string(semconv.DBStatementKey), rediscmd.CmdString(cmd))

	ctx = opentracing.ContextWithSpan(ctx, span)

	return ctx, nil
}

func (h *opentracingHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	span := opentracing.SpanFromContext(ctx)

	if err := cmd.Err(); err != nil {
		if err != redis.Nil {
			ext.LogError(span, err)
		}
	}

	span.Finish()
	return nil
}

func (h *opentracingHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	summary, cmdsString := rediscmd.CmdsString(cmds)
	span, ctx := opentracing.StartSpanFromContextWithTracer(ctx, h.tracer, "pipeline "+summary)

	ext.SpanKindRPCClient.Set(span)

	// to maintain compatibility with opentelemetry convention
	span.SetTag(string(semconv.DBSystemRedis.Key), semconv.DBSystemRedis.Value.AsString())
	span.SetTag(string(semconv.DBRedisDBIndexKey), h.config.Database)
	span.SetTag(string(semconv.NetTransportTCP.Key), semconv.NetTransportTCP.Value.AsString())
	span.SetTag(string(semconv.NetPeerNameKey), h.config.Host)
	span.SetTag(string(semconv.NetPeerPortKey), h.config.Port)
	span.SetTag(string(semconv.DBOperationKey), summary)
	span.SetTag(string(semconv.DBStatementKey), cmdsString)
	span.SetTag("db.redis.num_cmd", len(cmds))

	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx, nil
}

func (h *opentracingHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	span := opentracing.SpanFromContext(ctx)
	if err := cmds[0].Err(); err != nil {
		if err != redis.Nil {
			ext.LogError(span, err)
		}
	}

	span.Finish()
	return nil
}

// Client is the interface returned by Wrap.
//
// Client implements redis.UniversalClient
type Client interface {
	redis.UniversalClient

	// ClusterClient returns the wrapped *redis.ClusterClient,
	// or nil if a non-cluster client is wrapped.
	Cluster() *redis.ClusterClient

	// Ring returns the wrapped *redis.Ring,
	// or nil if a non-ring client is wrapped.
	RingClient() *redis.Ring
}

// Wrap wraps client such that executed commands are reported as spans to tracer,
// using the command's associated context.
func Wrap(client redis.UniversalClient, tracer opentracing.Tracer, config Config) Client {
	if tracer == nil {
		tracer = opentracing.GlobalTracer()
	}

	client.AddHook(&opentracingHook{tracer: tracer, config: config})

	switch client.(type) {
	case *redis.Client:
		return contextClient{Client: client.(*redis.Client), tracer: tracer}
	case *redis.ClusterClient:
		return contextClusterClient{ClusterClient: client.(*redis.ClusterClient), tracer: tracer}
	case *redis.Ring:
		return contextRingClient{Ring: client.(*redis.Ring), tracer: tracer}
	}

	return client.(Client)
}

type contextClient struct {
	*redis.Client
	tracer opentracing.Tracer
	config Config
}

func (c contextClient) Cluster() *redis.ClusterClient {
	return nil
}

func (c contextClient) RingClient() *redis.Ring {
	return nil
}

type contextClusterClient struct {
	*redis.ClusterClient
	tracer opentracing.Tracer
	config Config
}

func (c contextClusterClient) Cluster() *redis.ClusterClient {
	return c.ClusterClient
}

func (c contextClusterClient) RingClient() *redis.Ring {
	return nil
}

type contextRingClient struct {
	*redis.Ring
	tracer opentracing.Tracer
	config Config
}

func (c contextRingClient) Cluster() *redis.ClusterClient {
	return nil
}

func (c contextRingClient) RingClient() *redis.Ring {
	return c.Ring
}
