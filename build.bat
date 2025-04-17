@echo off
echo Generating gRPC code...
protoc --go_out=proto --go-grpc_out=proto --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative -I proto proto\*.proto

echo Building gRPC server...
go build -o gopass-grpc.exe cmd\server\main.go

echo Building CLI...
go build

echo Done.
