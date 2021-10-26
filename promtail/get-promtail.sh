#!/bin/bash

set -e
curl -O -L "https://github.com/grafana/loki/releases/download/v2.3.0/promtail-linux-amd64.zip"
unzip "promtail-linux-amd64.zip"
chmod a+x "promtail-linux-amd64"
mv promtail-linux-amd64 promtail
rm promtail-linux-amd64.zip
