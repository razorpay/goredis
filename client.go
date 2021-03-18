package otredis

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/extra/rediscmd"
	"github.com/go-redis/redis/v8"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
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

	ext.DBType.Set(span, "redis")
	ext.SpanKindRPCClient.Set(span)
	ext.DBStatement.Set(span, rediscmd.CmdString(cmd))
	ext.PeerService.Set(span, h.config.Host)
	ext.PeerAddress.Set(span, h.config.Host+":"+strconv.Itoa(int(h.config.Port)))
	ext.PeerPort.Set(span, h.config.Port)

	// to maintain compatibility with opentelemetry convention
	span.SetTag("db.system", "redis")

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

	ext.DBType.Set(span, "redis")
	ext.SpanKindRPCClient.Set(span)
	ext.DBStatement.Set(span, cmdsString)
	ext.PeerService.Set(span, h.config.Host)
	ext.PeerAddress.Set(span, h.config.Host+":"+strconv.Itoa(int(h.config.Port)))
	ext.PeerPort.Set(span, h.config.Port)


	// to maintain compatibility with opentelemetry convention
	span.SetTag("db.system", "redis")
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

// Wrap wraps client such that executed commands are reported as spans to Elastic APM,
// using the client's associated context.
// A context-specific client may be obtained by using Client.WithContext.
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
