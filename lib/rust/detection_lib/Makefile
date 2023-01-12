setup:
	{ \
	set -e ;\
	(test -d .venv || python -m venv .venv);\
	if [ "$$(uname)" = "Darwin" ]; then \
		.venv/bin/pip install "maturin[zig]";\
		echo "Make sure to install patchelf (e.g. using brew)";\
	else \
		.venv/bin/pip install "maturin[zig,patchelf]";\
	fi; \
	}

build:
	PYO3_CROSS_PYTHON_VERSION=3.9 .venv/bin/python -m maturin build --release --strip --zig --compatibility manylinux2014 -i python3.9 --target x86_64-unknown-linux-gnu

release: setup
	$(MAKE) build
