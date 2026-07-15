.PHONY: all proto

proto:
	protoc --go_out=. --go_opt=module=github.com/VitoNaychev/egt-challenge \
	--go-grpc_out=. --go-grpc_opt=module=github.com/VitoNaychev/egt-challenge \
	pkg/proto/event.proto \
	persistence/proto/event_service.proto

