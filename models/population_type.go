package models

type PopulationType struct {
	Name string `json:"name"`
}

func NewPopulationType(name string) PopulationType {
	return PopulationType{name}
}
