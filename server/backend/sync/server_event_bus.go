package sync

import (
	"context"
	gotime "time"

	"github.com/yorkie-team/yorkie/api/types"
)

type ServerEventBus interface {
	IsSplitBrainInCluster(ctx context.Context) (bool, error)
	Publish(ctx context.Context, publisherID types.ID, event ServerEvent)
	StoreSubscription(ctx context.Context, subscription *Subscription) error
	Close() error
}

type ServerEvent struct {
	Type        types.ServerEventType
	EventID     types.ID
	Publisher   types.ID
	PublishedAt gotime.Time
}
