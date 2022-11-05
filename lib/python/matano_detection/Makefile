SHELL := /bin/bash

release:
	{ \
    set -e ;\
    OUTDIR=$$(echo ${CURDIR}/../../../local-assets/MatanoDetectionsCommonLayer);\
	mkdir -p $$OUTDIR ;\
	rm -rf ./detection/__pycache__;\
	python3 -m pip install --no-user --disable-pip-version-check --no-compile --force-reinstall --upgrade -r requirements.txt --platform manylinux2014_x86_64 --implementation cp --python-version 3.9 --only-binary=:all: -t $$OUTDIR/python && cp -a ./detection $$OUTDIR/python ;\
    }
