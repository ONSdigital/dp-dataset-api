package permissions

// CRUD is a representation of permissions required by an endpoint or held by a user/service.
type CRUD struct {
	Create bool
	Read   bool
	Update bool
	Delete bool
}

func (required *CRUD) Check(caller *CRUD) bool {
	return false
}
