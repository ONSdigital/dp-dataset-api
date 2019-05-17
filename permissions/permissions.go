package permissions

//CRUD is a representation of permissions required by an endpoint or held by a user/service.
type CRUD struct {
	Create bool
	Read   bool
	Update bool
	Delete bool
}

func (c CRUD) IsCreate() bool {
	return c.Create
}

func (c CRUD) IsRead() bool {
	return c.Read
}

func (c CRUD) IsUpdate() bool {
	return c.Update
}

func (c CRUD) IsDelete() bool {
	return c.Delete
}
