#!/bin/sh

set -e

ROOT=$(readlink -f $(dirname "$0")/..)

./run.sh listUserAgents > $ROOT/output/rawuastats

./run.sh fixIPs
./run.sh tagRequestUsers
./run.sh tranow_classifyUsers Fakultätsrat | sort > $ROOT/output/classification_tranow.csv
./run.sh tobias_poll > $ROOT/output/tobias_poll
./run.sh tobias_activityPhases Fakultätsrat \
	Doktorand/in,Mittelbau,Professor/in \
	'BMFZ,Verwaltung,Promovierendenvertreter/in iGRAD,Studierende,Vertreter der Medizinischen Fakultät,Vertreter aus Koordinierten Programmen,Weitere Mitarbeiterinnen und Mitarbeiter' \
	treatment-email-0,treatment-email-1 \
	Mathematik,Chemie,Informatik,Medizin,Biologie,Psychologie,Pharmazie,Physik,Geographie \
	> $ROOT/output/tobias_activityPhases
./run.sh participationStats_badges Doktorand/in Mittelbau Professor/in > $ROOT/output/matthias_rough