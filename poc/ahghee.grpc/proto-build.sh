#!/usr/bin/env bash

packages/Grpc.Tools.1.11.x/tools/linux_x64/protoc --csharp_out ./ --plugin=protoc-gen-grpc=packages/Grpc.Tools.1.11.x/tools/linux_x64/grpc_csharp_plugin ./types.proto 