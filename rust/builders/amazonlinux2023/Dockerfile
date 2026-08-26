FROM amazonlinux:2023
# Find architecture of host, e.g. amd64 or arm64
ARG BUILDARCH
ARG RUST_VERSION=1.98.0

RUN mkdir -p /opt/sysroot/x86_64/var/cache /opt/sysroot/aarch64/var/cache

# Avoid downloading hosts dnf metadata cache again by symlinking the cache from container into the sysroot
# matching the host architecture
RUN case "${BUILDARCH}" in \
    "amd64") ln -s /var/cache/dnf /opt/sysroot/x86_64/var/cache ;;\
    "arm64") ln -s /var/cache/dnf /opt/sysroot/aarch64/var/cache ;;\
    *) echo "Architecture ${BUILDARCH} not recognised"; exit 1;;\
    esac

# Install compiler and linker which supports x86_64 and aarch64 targets
RUN dnf makecache
RUN dnf install -y --setopt=install_weak_deps=False clang22 lld22 symlinks && dnf clean packages

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/.cargo \
    PATH=/usr/local/.cargo/bin:${PATH}

# Install Rust and add both required targets to toolchain
RUN curl --proto '=https' --tlsv1.2 --fail --show-error --silent --output /tmp/rustup-init.sh \
    https://sh.rustup.rs \
    && sh /tmp/rustup-init.sh -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION} \
    && rm -f /tmp/rustup-init.sh \
    && rustup target add aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu --toolchain ${RUST_VERSION}

# Create two sysroots, whichever one matches the host architecture should skip downloading repo metadata
RUN releasever="$(rpm -q system-release --qf '%{VERSION}')" \
    && dnf --releasever="${releasever}" --installroot=/opt/sysroot/aarch64 --forcearch=aarch64 \
    --setopt=install_weak_deps=False install -y \
    \
    system-release glibc-devel gcc gcc-c++ libstdc++-devel \
    \
    && dnf --releasever="${releasever}" --installroot=/opt/sysroot/aarch64 clean packages

RUN releasever="$(rpm -q system-release --qf '%{VERSION}')" \
    && dnf --releasever="${releasever}" --installroot=/opt/sysroot/x86_64 --forcearch=x86_64 \
    --setopt=install_weak_deps=False install -y \
    \
    system-release glibc-devel gcc gcc-c++ libstdc++-devel \
    \
    && dnf --releasever="${releasever}" --installroot=/opt/sysroot/x86_64 clean packages

# Convert absolute symlinks to relative within sysroots. DNF may create absolute symlinks
# pointing outside the sysroot, which breaks cross-compilation tooling at runtime.
RUN dnf clean all && symlinks -cr /opt/sysroot/

# Create compiler wrappers to specify sysroot directories and target architecture
RUN cat > /usr/local/bin/aarch64-linux-gnu-clang <<'EOF'
#!/bin/sh
exec clang-22 --target=aarch64-unknown-linux-gnu --sysroot=/opt/sysroot/aarch64 "$@"
EOF

RUN cat > /usr/local/bin/x86_64-linux-gnu-clang <<'EOF'
#!/bin/sh
exec clang-22 --target=x86_64-unknown-linux-gnu --sysroot=/opt/sysroot/x86_64 "$@"
EOF

RUN cat > /usr/local/bin/aarch64-linux-gnu-clang++ <<'EOF'
#!/bin/sh
exec clang++-22 --target=aarch64-unknown-linux-gnu --sysroot=/opt/sysroot/aarch64 "$@"
EOF

RUN cat > /usr/local/bin/x86_64-linux-gnu-clang++ <<'EOF'
#!/bin/sh
exec clang++-22 --target=x86_64-unknown-linux-gnu --sysroot=/opt/sysroot/x86_64 "$@"
EOF

RUN chmod 0755 /usr/local/bin/aarch64-linux-gnu-clang /usr/local/bin/x86_64-linux-gnu-clang \
    /usr/local/bin/aarch64-linux-gnu-clang++ /usr/local/bin/x86_64-linux-gnu-clang++

ENV CC_aarch64_unknown_linux_gnu=/usr/local/bin/aarch64-linux-gnu-clang \
    CC_x86_64_unknown_linux_gnu=/usr/local/bin/x86_64-linux-gnu-clang \
    CXX_aarch64_unknown_linux_gnu=/usr/local/bin/aarch64-linux-gnu-clang++ \
    CXX_x86_64_unknown_linux_gnu=/usr/local/bin/x86_64-linux-gnu-clang++

# Configure Rust to use per-target cross-compilation wrappers and lld linker
# The linker path points to our clang/clang++ wrappers (which inject --target and --sysroot)
# rustflags adds -fuse-ld=lld only at link time to avoid "unused parameter" errors during C compilation
RUN mkdir -p /usr/local/.cargo \
    && cat > /usr/local/.cargo/config.toml <<'EOF'
[target.aarch64-unknown-linux-gnu]
linker = "/usr/local/bin/aarch64-linux-gnu-clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]

[target.x86_64-unknown-linux-gnu]
linker = "/usr/local/bin/x86_64-linux-gnu-clang"
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
EOF

WORKDIR /workspace