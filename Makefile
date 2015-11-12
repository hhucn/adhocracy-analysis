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

run: prepare
	./ay list_uas --summarize > output/uas
	./ay session_user_stats
	./ay basicfacts

export:
	./ay tobias_export --output output.xlsx --include-proposals

.PHONY: test
