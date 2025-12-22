package cloudflare

import (
	"context"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/cache"
)

const maxPrefixesPerPurge = 30

// PurgeByPrefixes purges the Cloudflare cache for the given URL prefixes
// Prefixes are sent in batches of maxPrefixesPerPurge to comply with Cloudflare API limits
func (c *Client) PurgeByPrefixes(ctx context.Context, prefixes []string) error {
	for i := 0; i < len(prefixes); i += maxPrefixesPerPurge {
		end := min(i+maxPrefixesPerPurge, len(prefixes))
		batch := prefixes[i:end]

		params := cache.CachePurgeParams{
			ZoneID: cloudflare.F(c.ZoneID),
			Body: cache.CachePurgeParamsBody{
				Prefixes: cloudflare.F[any](batch),
			},
		}

		if _, err := c.CacheService.Purge(ctx, params); err != nil {
			return err
		}
	}

	return nil
}
