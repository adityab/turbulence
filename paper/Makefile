CONFIG=config.txt
IN=turbulence.rst
OUT=turbulence.pdf
TEX=$(addsuffix .tex,$(basename $(OUT)))

OPTIONS=--title=Modernity --use-latex-docinfo --config=$(CONFIG) --use-latex-citations --use-latex-toc

$(OUT): $(CONFIG) $(IN)
	rst2latex.py $(OPTIONS) $(IN) > $(TEX)
	# always run twice to get x-refs in order
	pdflatex $(TEX)
	pdflatex $(TEX)

clean:
	rm -rf *.pdf *.out *.log *.tex *.aux *.toc
