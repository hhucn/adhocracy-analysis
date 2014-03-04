test:
	nosetests test/
	pep8 .

run:
	./ay cleanup_requestlog
	./ay list_uas --summarize > output/uas

.PHONY: test
