#!/bin/sh

export GOPATH=$(readlink -f $(dirname "$0"))
go run $GOPATH/src/cn.hhu.de/*.go "$@"
