# The correct AWS SAM build image based on the runtime of the function will be
# passed as build arg. The default allows to do `docker build .` when testing.
ARG IMAGE=public.ecr.aws/sam/build-nodejs14.x
FROM $IMAGE
# -nightlyesbui
ARG rust_version=nightly

# Install typescript
RUN npm install --global typescript

# Install esbuild
# (unsafe-perm because esbuild has a postinstall script)
ARG ESBUILD_VERSION=0
RUN npm install --global --unsafe-perm=true esbuild@$ESBUILD_VERSION

# Ensure all users can write to npm cache
RUN mkdir /tmp/npm-cache && \
    chmod -R 777 /tmp/npm-cache && \
    npm config --global set cache /tmp/npm-cache


# Disable npm update notifications
RUN npm config --global set update-notifier false

# create non root user and change allow execute command for non root user
RUN /sbin/useradd -u 1000 user && chmod 711 /

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN yum install -y jq gcc gcc-c++ openssl-devel && \
    curl --fail https://sh.rustup.rs -sSf \
        | sh -s -- -y --default-toolchain ${rust_version}-x86_64-unknown-linux-gnu --no-modify-path && \
    rustup default ${rust_version}-x86_64-unknown-linux-gnu && \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; 

CMD [ "esbuild" ]