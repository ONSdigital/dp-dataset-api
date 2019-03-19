package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/observation"
)

func (m *Mock) StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error) {
	return nil, m.checkForErrors()
}
