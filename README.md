# goredis
a middleware for goredis to use opentracing

```go
import (
	"net/http"

	"github.com/go-redis/redis/v8"

	otredis "github.com/krantideep95/goredis"
)

var redisClient *redis.Client // initialized at program startup

func handleRequest(w http.ResponseWriter, req *http.Request) {

	client := otredis.Wrap(redisClient, nil, , otredis.Config{
        Host:     host,
        Port:     uint16(port),
        Database: database,
})
	...
}
```
