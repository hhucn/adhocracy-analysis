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

prepare:
	./ay cleanup_requestlog
	./ay annotate_requests
	./ay assign_requestlog_sessions --timeout 600

run: prepare
	./ay list_uas --summarize > output/uas
	./ay session_user_stats
	./ay basicfacts

export_habil15:
	./ay tobias_export_habil15 --output output/output.xlsx --include-proposals --config .config_2015.json

export_habil15_short:
	./ay tobias_export_habil15 --output output/output_no_proposals.xlsx --config .config_2015.json

export_promo16:
	./ay tobias_export_promo16 --output output/output.xlsx --include-proposals --config .config_2016.json

export_promo16_short:
	./ay tobias_export_promo16 --output output/output_no_proposals.xlsx --config .config_2016.json

.PHONY: test
