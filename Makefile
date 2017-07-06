test:
	nosetests test/
	pep8 .

import:
	./ay load_requestlog

dependencies:
	# Check for python
	python -c 0 > /dev/null

	pip3.4 install --user pygeoip
	pip3.4 install --user matplotlib

switch-habil15:
	rm ./.config.json
	ln -s ./.config_2015.json ./.config.json

switch-promo16:
	rm ./.config.json
	ln -s ./.config_2016.json ./.config.json

switch-promo16-1:
	rm ./.config.json
	ln -s ./.config_2016_1.json ./.config.json

switch-promo16-2:
	rm ./.config.json
	ln -s ./.config_2016_2.json ./.config.json

prepare:
	./ay cleanup_requestlog
	./ay annotate_requests
	./ay assign_requestlog_sessions --timeout 600

run: prepare
	./ay list_uas --summarize > output/uas
	./ay session_user_stats
	./ay basicfacts

export_habil15:
	./ay tobias_export_habil15 --output output/output.xlsx --include-proposals

export_habil15_short:
	./ay tobias_export_habil15 --output output/output_no_proposals.xlsx

export_promo16:
	./ay tobias_export_promo16 --output output/output.xlsx --include-proposals

export_promo16_short:
	./ay tobias_export_promo16 --output output/output_no_proposals.xlsx

.PHONY: test
