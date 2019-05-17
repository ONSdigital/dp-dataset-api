package log

type eventAuth struct {
	Identity     string       `json:"identity,omitempty"`
	IdentityType identityType `json:"identity_type,omitempty"`
}

type identityType string

const (
	// SERVICE represents a service account type
	SERVICE identityType = "service"
	// USER represents a user account type
	USER identityType = "user"
)

func (l *eventAuth) attach(le *EventData) {
	le.Auth = l
}

// Auth returns an option you can pass to Event to include identity information,
// for example the identity type and user/service ID from an inbound HTTP request
func Auth(identityType identityType, identity string) option {
	return &eventAuth{
		Identity:     identity,
		IdentityType: identityType,
	}
}
