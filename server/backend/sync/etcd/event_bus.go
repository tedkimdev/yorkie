package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	gotime "time"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/server/backend/sync"
	"github.com/yorkie-team/yorkie/server/logging"
)

const (
	eventsPath         = "/events/servers/"
	serverUpCountKey   = "server-up-count"
	serverDownCountKey = "server-down-count"
)

type etcdServerEvent struct {
	Type        string      `json:"type"`
	EventID     string      `json:"event_id"`
	Publisher   string      `json:"publisher"`
	PublishedAt gotime.Time `json:"published_at"`
}

// Publish publishes the given event.
func (c *Client) Publish(
	ctx context.Context,
	publisherID types.ID,
	event sync.ServerEvent,
) {
	// put event to etcd
	serverEvent := etcdServerEvent{
		Type:        string(event.Type),
		Publisher:   event.Publisher.String(),
		EventID:     event.EventID.String(),
		PublishedAt: event.PublishedAt,
	}
	// put server up count
	if serverEvent.Type == string(types.ServerUpEvent) {
		upCount, err := c.getServerUpCount(ctx)
		if err != nil {
			logging.DefaultLogger().Error(err)
			return
		}
		updatedBytes := []byte(strconv.Itoa(upCount + 1))
		_, err = c.client.Put(ctx, serverUpCountKey, string(updatedBytes))
		if err != nil {
			logging.DefaultLogger().Errorf("put %s: %w", serverUpCountKey, err)
			return
		}
	}

	// put server down count
	if serverEvent.Type == string(types.ServerDownEvent) {
		downCount, err := c.getServerDownCount(ctx)
		if err != nil {
			logging.DefaultLogger().Error(err)
			return
		}
		updatedBytes := []byte(strconv.Itoa(downCount + 1))
		_, err = c.client.Put(ctx, serverDownCountKey, string(updatedBytes))
		if err != nil {
			logging.DefaultLogger().Errorf("put %s: %w", serverDownCountKey, err)
			return
		}
	}

	bytes, err := json.Marshal(serverEvent)
	if err != nil {
		logging.DefaultLogger().Errorf("marshal %s: %w", c.serverInfo.ID, err)
		return
	}

	// put server up/down event
	k := path.Join(eventsPath, serverEvent.EventID) // key: /events/server/{event-id}

	// grantResponse, err := c.client.Grant(ctx, int64(serverValueTTL.Seconds()))
	// if err != nil {
	// 	logging.DefaultLogger().Errorf("grant %s: %w", c.serverInfo.ID, err)
	// 	return
	// }
	// _, err = c.client.Put(ctx, k, string(bytes), clientv3.WithLease(grantResponse.ID))
	_, err = c.client.Put(ctx, k, string(bytes))
	if err != nil {
		logging.DefaultLogger().Errorf("put %s: %w", k, err)
		return
	}
}

func (c *Client) IsSplitBrainInCluster(ctx context.Context) (bool, error) {
	// TODO: return zero value false? or true?
	upCount, err := c.getServerUpCount(ctx)
	if err != nil {
		logging.DefaultLogger().Error(err)
		return false, err
	}
	downCount, err := c.getServerDownCount(ctx)
	if err != nil {
		logging.DefaultLogger().Error(err)
		return false, err
	}

	memberCount := c.MaxMembersCount()
	return memberCount != (upCount - downCount), nil
}

func (c *Client) StoreSubscription(ctx context.Context, subscription *sync.Subscription) error {
	c.docSubscriptionsMu.Lock()
	defer c.docSubscriptionsMu.Unlock()

	c.docSubscriptions = append(c.docSubscriptions, subscription)
	return nil
}

func (c *Client) getServerDownCount(ctx context.Context) (int, error) {
	resp, err := c.client.Get(ctx, serverDownCountKey)
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", serverDownCountKey, err)
	}
	if len(resp.Kvs) < 1 {
		logging.DefaultLogger().Error(fmt.Errorf("kv not found in %s", serverDownCountKey))
		return 0, nil
	}

	currentValue := resp.Kvs[0].Value
	currentCount, err := strconv.Atoi(string(currentValue))
	if err != nil {
		return 0, fmt.Errorf("convert current value to integer: %w", err)
	}
	return currentCount, nil
}

func (c *Client) getServerUpCount(ctx context.Context) (int, error) {
	resp, err := c.client.Get(ctx, serverUpCountKey)
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", serverUpCountKey, err)
	}
	if len(resp.Kvs) < 1 {
		logging.DefaultLogger().Error(fmt.Errorf("kv not found in %s", serverUpCountKey))
		return 0, nil
	}

	currentValue := resp.Kvs[0].Value
	currentCount, err := strconv.Atoi(string(currentValue))
	if err != nil {
		return 0, fmt.Errorf("convert current value to integer: %w", err)
	}
	return currentCount, err
}
