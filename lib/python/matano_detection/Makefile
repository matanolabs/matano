SHELL := /bin/bash

release:
	{ \
    set -e ;\
	BASE_DIR=$$(echo ${CURDIR}/../../..);\
    OUTDIR=$$(echo $$BASE_DIR/local-assets/MatanoDetectionsCommonLayer);\
	mkdir -p $$OUTDIR ;\
	RUST_WHEEL_LIB_PATH=$$(echo $$BASE_DIR/lib/rust/target/wheels/detection_lib-0.1.0-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl);\
	rm -rf ./detection/__pycache__;\
	python3 -m pip install --no-user --disable-pip-version-check --no-compile --force-reinstall --upgrade --platform manylinux2014_x86_64 --implementation cp --python-version 3.9 --only-binary=:all: -t $$OUTDIR/python $$RUST_WHEEL_LIB_PATH;\
	python3 -m pip install --no-user --disable-pip-version-check --no-compile --force-reinstall --upgrade -r requirements.txt --platform manylinux2014_x86_64 --implementation cp --python-version 3.9 --only-binary=:all: -t $$OUTDIR/python && cp -a ./detection $$OUTDIR/python ;\
    }
