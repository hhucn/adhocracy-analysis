#!/bin/sh

set -e

ROOT=$(readlink -f $(dirname "$0")/..)

./run.sh listUserAgents > $ROOT/output/rawuastats

./run.sh tagRequestUsers
