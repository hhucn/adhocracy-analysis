test:
	nosetests test/
	pep8 .

import:
	./ay load_requestlog input/access_log_adhocracy --discard output/discards

prepare:
	./ay cleanup_requestlog
	./ay annotate_requests

run: prepare
	./ay list_uas --summarize > output/uas
	./ay session_stats

.PHONY: test
