#!/usr/bin/env bash

packages/Grpc.Tools.1.11.x/tools/linux_x64/protoc --csharp_out ./Ekati/Protocol/Grpc/ --plugin=protoc-gen-grpc=grpc_csharp_plugin ./protocol.grpc.proto 