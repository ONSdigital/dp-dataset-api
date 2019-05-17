package permissions

const (
	// Create permission
	CREATE Permission = "CREATE"
	// Read permission
	READ Permission = "READ"
	// Update permission
	UPDATE Permission = "UPDATE"
	// Delete permission
	DELETE Permission = "DELETE"
)

type Permission string

//Permissions is a representation of a CRUD permissions required by an endpoint or held by a user/service.
type Permissions struct {
	Perms []Permission
}

// Required is a convenience method for creating a Permissions object. Add permissions by supplying true or false for
// each of the CRUD values.
func Required(Create bool, read bool, update bool, delete bool) Permissions {
	required := make([]Permission, 0)

	if Create {
		required = append(required, CREATE)
	}
	if read {
		required = append(required, READ)
	}
	if update {
		required = append(required, UPDATE)
	}
	if delete {
		required = append(required, DELETE)
	}

	return Permissions{Perms: required}
}
