test:
	nosetests test/
	pep8 .

import:
	./ay load_requestlog input/access_log_adhocracy --discard output/discards

dependencies:
	# Check for python
	python -c 0 > /dev/null

	pip3 install --user pygeoip
	pip3 install --user matplotlib

prepare:
	./ay cleanup_requestlog
	./ay annotate_requests

run: prepare
	./ay list_uas --summarize > output/uas
	./ay session_stats
	./ay basicfacts

.PHONY: test
