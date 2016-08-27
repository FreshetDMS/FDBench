
.PHONY: design.pdf all clean

all: design.pdf

%.tex: %.raw
	./raw2tex $< > $@

%.tex: %.dat
	./dat2tex $< > $@

design.pdf: design.tex
	latexmk -pdf -pdflatex="pdflatex -interactive=nonstopmode" -use-make design.tex

clean:
	latexmk -CA
