SRC := $(shell find . -name *.tex) 
IMG := $(shell find . -name *.png)
TOR := *.dvi *.log *.toc *.pdf *.bbl *.blg $(SRC:.tex=.aux)

default: book.dvi

book.dvi: $(SRC) $(IMG)
	pdflatex main
	bibtex main
	pdflatex main
	pdflatex main

.PHONY: clean
clean:
	rm -f $(TOR)
