@echo off
echo Generating gRPC code...
protoc --go_out=proto --go-grpc_out=proto --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative -I proto proto\*.proto

echo Building gRPC server...
go build -o gopass-grpc.exe cmd\server\main.go

echo Copying executable to target directory...
copy gopass-grpc.exe ..\DecentPass\DecentPass\Platforms\Windows

echo Building CLI...
go build
copy gopass.exe ..\DecentPass\DecentPass\Platforms\Windows

echo Done.
