#!/bin/bash
rm manager_dc.zip
GIT_COMMIT=$(git rev-list -1 HEAD)
BUILD=$(date +'%Y-%m-%d')
VERSION=$1
rm manager_dc.$VERSION

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o manager_dc.$VERSION -ldflags "-X main.version=$VERSION -X main.commit=$GIT_COMMIT -X main.build=$BUILD"
if [ -f manager_dc.$VERSION ] ; then
	zip manager_dc.zip manager_dc.$VERSION
fi
