
setup:
	(test -d .venv || python -m venv .venv) && .venv/bin/pip install "maturin[zig,patchelf]"

build:
	.venv/bin/python -m maturin build --release --strip --zig --compatibility manylinux2014 -i python3.9

release: setup
	$(MAKE) build
