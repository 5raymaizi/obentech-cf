#!/bin/bash
rm cf_arbitrage_dc.zip
GIT_COMMIT=$(git rev-list -1 HEAD)
BUILD=$(date +'%Y-%m-%d')
VERSION=$1
rm cf_arbitrage_dc.$VERSION

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cf_arbitrage_dc.$VERSION -ldflags "-X main.version=$VERSION -X main.commit=$GIT_COMMIT -X main.build=$BUILD"
if [ -f cf_arbitrage_dc.$VERSION ] ; then
	zip cf_arbitrage_dc.zip cf_arbitrage_dc.$VERSION
fi
