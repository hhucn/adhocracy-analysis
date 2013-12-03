#!/bin/sh

set -e

ROOT=$(readlink -f $(dirname "$0")/..)

./run.sh listUserAgents > $ROOT/output/rawuastats

./run.sh fixIPs
./run.sh tagRequestUsers
./run.sh tranow_classifyUsers Fakultätsrat | sort > $ROOT/output/classification_tranow.csv

