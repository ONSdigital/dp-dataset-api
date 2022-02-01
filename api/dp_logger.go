package api

import (
	"context"
	"github.com/ONSdigital/log.go/v2/log"
)

type dpLogger struct {
}

func (d dpLogger) Error(ctx context.Context, event string, err error) {
	log.Error(ctx, event, err)
}
