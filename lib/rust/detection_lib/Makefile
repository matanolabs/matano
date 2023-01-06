
setup:
	(test -d .venv || python -m venv .venv) && . .venv/bin/activate && pip install "maturin[zig,patchelf]"

build:
	. .venv/bin/activate && maturin build --release --strip --zig --compatibility manylinux2014 -i python3.9

release: setup build
