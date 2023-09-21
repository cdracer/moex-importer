.PHONY: deploy build

deploy: build commit

build:
	rm dist/*
	python3 setup.py sdist bdist_wheel
	pdoc -o docs -d numpy ./moeximporter 
