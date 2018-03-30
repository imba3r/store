package thunder

type Thunder struct {
	Store        Store
	EventHandler EventHandler
}

func New(store Store, logEvents bool) *Thunder {
	eh := newEventHandler(logEvents);
	return &Thunder{
		Store:        newAdapter(store, eh),
		EventHandler: eh,
	}
}
