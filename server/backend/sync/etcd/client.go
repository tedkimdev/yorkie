package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	gosync "sync"
	gotime "time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/server/backend/sync"
	"github.com/yorkie-team/yorkie/server/logging"
)

const (
	sendReconnectMessagePeriod = 5 * gotime.Second
)

// Client is a client that connects to ETCD.
type Client struct {
	config     *Config
	serverInfo *sync.ServerInfo

	client *clientv3.Client

	docSubscriptionsMu *gosync.RWMutex
	docSubscriptions   []*sync.Subscription

	memberMapMu    *gosync.RWMutex
	memberMap      map[string]*sync.ServerInfo
	maxMemberCount int

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// newClient creates a new instance of Client.
func newClient(
	conf *Config,
	serverInfo *sync.ServerInfo,
) *Client {
	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Client{
		config:     conf,
		serverInfo: serverInfo,

		docSubscriptionsMu: &gosync.RWMutex{},
		docSubscriptions:   make([]*sync.Subscription, 0),

		memberMapMu:    &gosync.RWMutex{},
		memberMap:      make(map[string]*sync.ServerInfo),
		maxMemberCount: 0,

		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

// Dial creates a new instance of Client and dials the given ETCD.
func Dial(
	conf *Config,
	serverInfo *sync.ServerInfo,
) (
	*Client, error) {
	c := newClient(conf, serverInfo)

	if err := c.Dial(); err != nil {
		return nil, err
	}

	return c, nil
}

// Dial dials the given ETCD.
func (c *Client) Dial() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   c.config.Endpoints,
		DialTimeout: c.config.ParseDialTimeout(),
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		Username:    c.config.Username,
		Password:    c.config.Password,
	})
	if err != nil {
		return fmt.Errorf("connect to etcd: %w", err)
	}

	logging.DefaultLogger().Infof("etcd connected, URI: %s", c.config.Endpoints)

	c.client = cli
	return nil
}

// Close all resources of this client.
func (c *Client) Close() error {
	c.cancelFunc()

	if err := c.removeServerInfo(context.Background()); err != nil {
		logging.DefaultLogger().Error(err)
	}
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("close etcd client: %w", err)
	}

	return nil
}

func (c *Client) Initialize() error {
	ctx := context.Background()
	if err := c.putServerInfo(ctx); err != nil {
		return err
	}
	if err := c.initializeMemberMap(ctx); err != nil {
		return err
	}

	go c.syncServerInfos()
	go c.putServerPeriodically()
	go c.watchServerEvent()

	return nil
}

func (c *Client) watchServerEvent() {
	watchCh := c.client.Watch(c.ctx, eventsPath, clientv3.WithPrefix())
	for {
		select {
		case watchResponse := <-watchCh:
			for _, event := range watchResponse.Events {
				if event.Type == clientv3.EventTypePut {
					var serverEvent etcdServerEvent
					if err := json.Unmarshal(event.Kv.Value, &serverEvent); err != nil {
						logging.DefaultLogger().Error(err)
						continue
					}
					switch types.ServerEventType(serverEvent.Type) {
					case types.ServerUpEvent:
						c.memberMapMu.Lock()
						if c.maxMemberCount < len(c.memberMap) {
							c.maxMemberCount = len(c.memberMap)
						}
						c.memberMapMu.Unlock()

						isSplitBrain, err := c.IsSplitBrainInCluster(c.ctx)
						if err != nil {
							logging.DefaultLogger().Error(err)
							continue
						}
						if !isSplitBrain {
							go c.sendReconnectMessage()
						}
					case types.ServerDownEvent:
						logging.DefaultLogger().Info("got a server down event from etcd")
					default:
						logging.DefaultLogger().Error("error unknown server event from etcd")
						continue
					}
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) sendReconnectMessage() {
	c.memberMapMu.Lock()
	subs := c.docSubscriptions
	c.docSubscriptions = make([]*sync.Subscription, 0)
	c.memberMapMu.Unlock()

	for i := 0; i < len(subs); i++ {
		select {
		case <-gotime.After(sendReconnectMessagePeriod):
			subs[i].Events() <- sync.DocEvent{
				Type: types.DocEventType(types.ServerReconnectEvent),
				// TODO: set publisher
			}
		case <-c.ctx.Done():
			return
		}
	}
}
