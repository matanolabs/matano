
setup:
	python -m venv .venv && . .venv/bin/activate && pip install "maturin[zig,patchelf]"

build:
	maturin build --strip --zig --compatibility manylinux2014 -i python3.9
