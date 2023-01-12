SHELL := /bin/bash
IMAGE ?= ghcr.io/pyo3/maturin
NAME ?= build_detection_lib

build:
	python3 -m venv .venv && source .venv/bin/activate && pip install "maturin[zig,patchelf]" && maturin build --strip --zig --compatibility manylinux2014 -i python3.9 --release;

build-docker:
	{ \
    set -e ;\
	BASE_DIR=$$(echo ${CURDIR}/../../..);\
	docker run -it --rm -v $$BASE_DIR:/io --entrypoint bash --name $(NAME) $(IMAGE) -c "cd lib/rust/detection_lib && python3 -m pip install maturin[zig] && maturin build --strip --zig --compatibility manylinux2014 -i python3.9 --release;";\
    }
# build on docker if OSX, otherwise build locally
release:
	{ \
	set -e ;\
	if [ "$$(uname)" = "Darwin" ]; then \
		make build-docker; \
	else \
		make build; \
	fi; \
	}
