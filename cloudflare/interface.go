package cloudflare

import (
	"context"
	"time"

	"github.com/cloudflare/cloudflare-go/v6/cache"
	"github.com/cloudflare/cloudflare-go/v6/option"
)

//go:generate moq -out mocks/client.go -pkg mocks . Clienter
//go:generate moq -out mocks/cache_service.go -pkg mocks . CacheService

// Clienter defines the interface for Cloudflare client operations
type Clienter interface {
	PurgeByPrefixes(ctx context.Context, prefixes []string) error
	GetTimeout() time.Duration
}

// CacheService defines the interface for Cloudflare cache service operations
type CacheService interface {
	Purge(ctx context.Context, params cache.CachePurgeParams, opts ...option.RequestOption) (*cache.CachePurgeResponse, error)
}
