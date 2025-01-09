package application

var Published = State{
	Name:      "published",
	EnterFunc: PublishVersion,
}

var EditionConfirmed = State{
	Name:      "edition-confirmed",
	EnterFunc: EditionConfirmVersion,
}

var Associated = State{
	Name:      "associated",
	EnterFunc: AssociateVersion,
}
