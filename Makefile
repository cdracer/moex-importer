.PHONY: deploy build

deploy: build

build:
	rm -f dist/*
	rm -f wiki/*
	rm -rf docs/*
	python3 setup.py sdist bdist_wheel
	pdoc -o docs -d numpy ./moeximporter
	pydoc-markdown -m MoexImporter -I moeximporter > wiki/moeximporter-wiki.md
	pydoc-markdown -m MoexSecurity -I moeximporter >> wiki/moeximporter-wiki.md
	pydoc-markdown -m MoexCandlePeriods -I moeximporter >> wiki/moeximporter-wiki.md
	pydoc-markdown -m MoexSessions -I moeximporter >> wiki/moeximporter-wiki.md
