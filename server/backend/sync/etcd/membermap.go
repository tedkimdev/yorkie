package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/yorkie-team/yorkie/server/backend/sync"
	"github.com/yorkie-team/yorkie/server/logging"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	serversPath         = "/servers"
	putServerInfoPeriod = 5 * time.Second
	serverValueTTL      = 7 * time.Second
)

// Members returns the members of this cluster.
func (c *Client) Members() map[string]*sync.ServerInfo {
	c.memberMapMu.RLock()
	defer c.memberMapMu.RUnlock()

	memberMap := make(map[string]*sync.ServerInfo)
	for _, member := range c.memberMap {
		memberMap[member.ID] = &sync.ServerInfo{
			ID:        member.ID,
			Hostname:  member.Hostname,
			UpdatedAt: member.UpdatedAt,
		}
	}

	return memberMap
}

func (c *Client) MaxMembersCount() int {
	c.memberMapMu.RLock()
	defer c.memberMapMu.RUnlock()

	return c.maxMemberCount
}

// initializeMemberMap initializes the local member map by loading data from etcd.
func (c *Client) initializeMemberMap(ctx context.Context) error {
	getResponse, err := c.client.Get(ctx, serversPath, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("get %s: %w", serversPath, err)
	}

	for _, kv := range getResponse.Kvs {
		var info sync.ServerInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			return fmt.Errorf("unmarshal %s: %w", kv.Key, err)
		}

		c.setServerInfo(string(kv.Key), info)
	}
	return nil
}

// putServerPeriodically puts the local server in etcd periodically.
func (c *Client) putServerPeriodically() {
	for {
		if err := c.putServerInfo(c.ctx); err != nil {
			logging.DefaultLogger().Error(err)
		}

		select {
		case <-time.After(putServerInfoPeriod):
		case <-c.ctx.Done():
			return
		}
	}
}

// putServerInfo puts the local server in etcd.
func (c *Client) putServerInfo(ctx context.Context) error {
	grantResponse, err := c.client.Grant(ctx, int64(serverValueTTL.Seconds()))
	if err != nil {
		return fmt.Errorf("grant %s: %w", c.serverInfo.ID, err)
	}

	serverInfo := *c.serverInfo
	serverInfo.UpdatedAt = time.Now()
	bytes, err := json.Marshal(serverInfo)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", c.serverInfo.ID, err)
	}

	k := path.Join(serversPath, c.serverInfo.ID)
	_, err = c.client.Put(ctx, k, string(bytes), clientv3.WithLease(grantResponse.ID))
	if err != nil {
		return fmt.Errorf("put %s: %w", k, err)
	}
	return nil
}

// removeServerInfo removes the local server in etcd.
func (c *Client) removeServerInfo(ctx context.Context) error {
	k := path.Join(serversPath, c.serverInfo.ID)
	_, err := c.client.Delete(ctx, k)
	if err != nil {
		return fmt.Errorf("remove %s: %w", k, err)
	}
	return nil
}

// syncServerInfos syncs the local member map with etcd.
func (c *Client) syncServerInfos() {
	// TODO(hackerwins): When the network is recovered, check if we need to
	// recover the channels watched in the situation.
	watchCh := c.client.Watch(c.ctx, serversPath, clientv3.WithPrefix())
	for {
		select {
		case watchResponse := <-watchCh:
			for _, event := range watchResponse.Events {
				k := string(event.Kv.Key)
				switch event.Type {
				case clientv3.EventTypePut:
					var info sync.ServerInfo
					if err := json.Unmarshal(event.Kv.Value, &info); err != nil {
						logging.DefaultLogger().Error(err)
						continue
					}
					c.setServerInfo(k, info)
				case clientv3.EventTypeDelete:
					c.deleteServerInfo(k)
				}
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// setServerInfo sets the given serverInfo to the local member map.
func (c *Client) setServerInfo(key string, value sync.ServerInfo) {
	c.memberMapMu.Lock()
	defer c.memberMapMu.Unlock()

	c.memberMap[key] = &value
}

// deleteServerInfo removes the given serverInfo from the local member map.
func (c *Client) deleteServerInfo(id string) {
	c.memberMapMu.Lock()
	defer c.memberMapMu.Unlock()

	delete(c.memberMap, id)
}
