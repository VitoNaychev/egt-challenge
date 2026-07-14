package repo

type EventModel struct {
	ID      string `db:"id"`
	Message string `db:"message"`
}
