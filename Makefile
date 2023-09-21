.PHONY: deploy build

deploy: build commit

build:
	rm dist/*
	python3 setup.py sdist bdist_wheel
	pdoc -o docs -d numpy ./moeximporter
	pydoc-markdown -m MoexImporter -I moeximporter > wiki/moeximporter-wiki.md
	pydoc-markdown -m MoexSecurity -I moeximporter > wiki/moexsecurity-wiki.md
