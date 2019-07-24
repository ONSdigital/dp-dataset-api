package log

import (
	"context"
)

type eventFuncMock struct {
	capCtx        context.Context
	capEvent      string
	capOpts       []option
	hasBeenCalled bool

	onEvent func(e eventFuncMock)
}

func (e *eventFuncMock) Event(ctx context.Context, event string, opts ...option) {
	e.capCtx = ctx
	e.capEvent = event
	e.capOpts = opts
	e.hasBeenCalled = true

	if e.onEvent != nil {
		e.onEvent(eventFuncMock{
			capCtx:        ctx,
			capEvent:      event,
			capOpts:       opts,
			hasBeenCalled: true,
		})
	}
}
