package util

import (
	"net"
	"os"
	"strconv"
	"time"
)

func GetRedisConn() (net.Conn, error) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	return net.Dial("tcp", redisAddr)
}

func GenRandString(prefix string) string {
	return prefix + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
}
