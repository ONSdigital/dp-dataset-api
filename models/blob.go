package models

type Blob struct {
	Name string `json:"name"`
}

func NewBlob(name string) Blob {
	return Blob{name}
}
