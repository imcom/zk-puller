package main

import (
	"os"
	"flag"
	"strings"
	"context"
	"github.com/golang/glog"
)

func init() {
}

func run() int {
	var zkHosts string
	var redisUrl string
	var paths string

	flag.StringVar(&zkHosts, "zookeepers", "localhost:2181", "zookeeper connection string")
	flag.StringVar(&redisUrl, "redis-url", "redis://localhost:6379/0", "redis URL string")
	flag.StringVar(&paths, "zk-paths", "", "path1,path2,path3/subpath1")

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	puller := InitPuller(strings.Split(zkHosts, ","), strings.Split(paths, ","),
		redisUrl, ctx, cancel)

	if err := puller.run(); err != nil {
		glog.Errorf("puller exited with error: %s", err)
		return 2
	} else {
		return 0
	}
}

func main() {
	os.Exit(run())
}
