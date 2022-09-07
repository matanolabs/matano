SHELL := /bin/bash

release:
	{ \
    set -e ;\
    OUTDIR=$$(echo ${CURDIR}/../../../local-assets/MatanoDetectionsCommonLayer);\
	mkdir -p $$OUTDIR ;\
	python -m pip install --disable-pip-version-check --no-compile --upgrade -r requirements.txt -t $$OUTDIR/python && cp -rT ./ $$OUTDIR/python ;\
    }
