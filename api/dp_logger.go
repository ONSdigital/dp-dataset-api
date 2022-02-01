package api

import (
	"context"
	"github.com/ONSdigital/log.go/v2/log"
)

type dpLogger struct {
}

func (d dpLogger) Error(ctx context.Context, event string, err error, data map[string]interface{}) {
	if data == nil {
		log.Error(ctx, event, err)
	} else {
		opt := log.Data{}
		for k, v := range data {
			opt[k] = v
		}
		log.Error(ctx, event, err, opt)
	}
}
