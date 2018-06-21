#!/usr/bin/env bash

temp_dir=packages/Grpc.Tools.1.11.x/tmp
curl_url=https://www.nuget.org/api/v2/package/Grpc.Tools/
mkdir -p $temp_dir && cd $temp_dir && curl -sL $curl_url > tmp.zip; unzip tmp.zip && cd .. && cp -r tmp/tools . && rm -rf tmp && cd ../..