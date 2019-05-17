package permissions

const (
	CREATE Permission = "CREATE"
	READ   Permission = "READ"
	UPDATE Permission = "UPDATE"
	DELETE Permission = "DELETE"
)

type Permission string

type Permissions struct {
	Required []Permission
}

type CallerPermissions struct {
	Permissions []Permission
}

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

	return Permissions{Required: required}
}
