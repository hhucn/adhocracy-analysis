#!/bin/sh

set -e

ROOT=$(readlink -f $(dirname "$0")/..)

./run.sh listUserAgents > $ROOT/output/rawuastats

./run.sh fixIPs
./run.sh tagRequestUsers
./run.sh tranow_classifyUsers Fakultätsrat | sort > $ROOT/output/classification_tranow.csv
./run.sh tobias_poll > $ROOT/output/tobias_poll
./run.sh tobias_activityPhases Fakultätsrat Doktorand/in,Mittelbau,Professor/in treatment-email-0,treatment-email-1 > $ROOT/output/tobias_activityPhases
