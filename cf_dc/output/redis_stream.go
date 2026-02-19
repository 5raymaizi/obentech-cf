package output

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

type (
	RedisStream struct {
		uniqueName string
		rds        *redis.Client
	}
)

func (rs *RedisStream) checkWrite(data *map[string]string) {
	if rs.rds.Get(fmt.Sprintf(`cf:%s:alive`, rs.uniqueName)).Err() != nil { // 有需要才导出html，节省redis流量
		return
	}
	jd, _ := json.Marshal(&data)
	rs.rds.Set(fmt.Sprintf(`cf:%s:orderbook`, rs.uniqueName), jd, 30*time.Minute)
}
