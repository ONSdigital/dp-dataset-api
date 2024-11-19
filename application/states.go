package application

var Published = State{
	Name:      "published",
	EnterFunc: PublishVersion,
}

var Submitted = State{
	Name:      "submitted",
	EnterFunc: SubmitVersion,
}

var Completed = State{
	Name:      "completed",
	EnterFunc: CompleteVersion,
}

var EditionConfirmed = State{
	Name:      "edition-confirmed",
	EnterFunc: EditionConfirmVersion,
}

var Associated = State{
	Name:      "associated",
	EnterFunc: AssociateVersion,
}

var Created = State{
	Name:      "created",
	EnterFunc: CreateVersion,
}

var Failed = State{
	Name:      "failed",
	EnterFunc: FailVersion,
}

var Detached = State{
	Name:      "detached",
	EnterFunc: DetachVersion,
}
