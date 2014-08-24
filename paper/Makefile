default: pdf

pdf:
	pdflatex paper

clean:
	rm -f paper.aux paper.log paper.pdf

cd: pdf
	xdg-open paper.pdf &

.PHONY: default pdf clean

