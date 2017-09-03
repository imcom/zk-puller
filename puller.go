package main

import (
	"github.com/go-redis/redis"
	"github.com/samuel/go-zookeeper/zk"
	"context"
	"time"
	"github.com/golang/glog"
	"net/url"
	"strconv"
	"fmt"
)

type Puller struct {
	ZkHosts  []string
	RedisUrl string
	Paths    []string

	zkCli    *zk.Conn
	zkChan   <-chan zk.Event

	redisCli *redis.Client

	ctx    context.Context
	cancel context.CancelFunc
}

// InitPuller returns a Puller instance
func InitPuller(zkHosts, paths []string, redisUrl string, ctx context.Context, cancel context.CancelFunc) (p *Puller) {
	puller := &Puller{
		ZkHosts:  zkHosts,
		RedisUrl: redisUrl,
		Paths:    paths,
		ctx:      ctx,
		cancel:   cancel,
	}

	if zkCli, zkChan, err := zk.Connect(zkHosts, 60*time.Second); err != nil {
		glog.Fatalf("failed to connect to Zookeeper, %s", err)
	} else {
		puller.zkCli = zkCli
		puller.zkChan = zkChan
	}

	if u, err := url.Parse(redisUrl); err != nil {
		glog.Fatalf("invalid redis connection string, %s", redisUrl)
	} else {
		db, _ := strconv.Atoi(u.Path[1:])
		redisCli := redis.NewClient(&redis.Options{
			Addr:     u.Host, // host:port
			Password: "",     // no password set
			DB:       db,     // use default DB
		})

		glog.Infof("connected to redis @ %s", redisUrl)

		puller.redisCli = redisCli
	}

	return puller
}

func (p *Puller) run() error {
	attempts := 0
	defer func() {
		glog.Infof("close all connections")
		p.zkCli.Close()
		p.redisCli.Close()
	}()

	for {
		select {
		case <-p.ctx.Done():
			glog.Infof("got cancel signal")
			return nil
		case e := <-p.zkChan:
			glog.Infof("received zk event: %v", e)
			switch e.State {
			case zk.StateConnecting:
				attempts += 1
				if attempts > 3 {
					return fmt.Errorf("too many retires")
				}
			case zk.StateConnected:
				glog.Infof("connected to Zookeeper @ %s after %d attempts", p.zkCli.Server(), attempts)
				attempts = 0
			case zk.StateDisconnected:
				glog.Warningf("zookeeper disconnected")
			}
		default:
			if p.zkCli.State() == zk.StateHasSession {
				glog.Infof("in looping to get policies from %s", p.Paths)
				for _, path := range p.Paths {
					if children, stat, err := p.zkCli.Children(path); err != nil {
						glog.Errorf("error occurred %s", err)
					} else {
						glog.Infof("got path stat: %v", stat)
						glog.Infof("got children %s from %s", children, path)
						for _, child := range children {
							node := fmt.Sprintf("%s/%s", path, child)
							_, cstat, err := p.zkCli.Get(node)
							if err != nil {
								glog.Errorf("failed to get %s, %s", node, err)
							} else {
								cstat_string := fmt.Sprintf("%s", cstat)
								p.redisCli.Set(node, cstat_string, 0)
							}
						}
						time.Sleep(1 * time.Second)
						//if len(children) == 1 {
						//	p.cancel()
						//}
					}
				}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return nil
}
