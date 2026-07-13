package handler

type Event struct {
	ID      string `json:"id" validate:"required"`
	Message string `json:"message" validate:"required"`
}

type ErrorResponse struct {
	Msg string `json:"error"`
}
