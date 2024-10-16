# A C++ Nvidia RAPIDS development image
FROM rapidsai/base:24.10-cuda12.5-py3.12 AS base

USER root
# Install tools
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y tzdata htop dstat less nano sudo software-properties-common wget unzip && apt-get clean

# Use shell form for redirection and enable sudo
ARG USERNAME=rapids
RUN echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME
RUN chmod 0440 /etc/sudoers.d/$USERNAME

# Drop to normal user
USER $USERNAME 
WORKDIR /home/rapids

FROM base AS build
USER root

# Install aws
RUN wget -q "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -O "awscliv2.zip" && unzip -q awscliv2.zip && ./aws/install && rm -rf aws awscliv2.zip 

# Enable gcc-13
RUN add-apt-repository ppa:ubuntu-toolchain-r/test

# Install certs and Amazon repository
RUN wget -O- "https://apt.corretto.aws/corretto.key" | apt-key add -
RUN printf 'deb https://apt.corretto.aws stable main' >/etc/apt/sources.list.d/corretto.list

# Install needed tools
RUN apt-get update && apt-get install -y java-17-amazon-corretto-jdk make cmake-format gcc-13 g++-13 \
    maven ccache cppcheck doxygen pkg-config git libboost-dev && apt-get clean

# Install certs and LLVM repository
RUN wget -O- "https://apt.llvm.org/llvm-snapshot.gpg.key" | apt-key add -
RUN printf 'deb http://apt.llvm.org/jammy/ llvm-toolchain-jammy-18 main\ndeb-src http://apt.llvm.org/jammy/ llvm-toolchain-jammy-18 main' >/etc/apt/sources.list.d/llvm.list

# Install Clang
RUN apt-get update && apt-get install -y clang-18 clang-format-18 clang-tidy-18 libclang-18-dev && apt-get clean

# Install CMake
RUN wget -q "https://github.com/Kitware/CMake/releases/download/v3.30.5/cmake-3.30.5-linux-x86_64.tar.gz" \
    && mkdir -p /opt/cmake \
    && tar --strip-components 1 -C /opt/cmake -zxf cmake-3.30.5-linux-x86_64.tar.gz \
    && rm -rf cmake-3.30.5-linux-x86_64.tar.gz \
    && echo "export PATH=\$PATH:/opt/cmake/bin" >"/etc/profile.d/cmake.sh" \
    && chmod 755 /etc/profile.d/cmake.sh

# Install include-what-you-use
RUN git clone https://github.com/include-what-you-use/include-what-you-use.git -b clang_18 \
    && /opt/cmake/bin/cmake -DCMAKE_PREFIX_PATH=/usr/lib/llvm-18 -S include-what-you-use/ -B build \
    && cd build \
    && make -j $(nproc) \
    && make install \
    && cd .. \
    && rm -rf include-what-you-use build

# Drop to normal user
USER $USERNAME 
WORKDIR /home/rapids

FROM build AS devcontainer
USER root
# This is to enable persistent BASH history
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && mkdir /commandhistory \
    && touch /commandhistory/.bash_history \
    && chown -R 1000 /commandhistory \
    && echo "$SNIPPET" >> "/home/$USERNAME/.bashrc"

# Drop to normal user
USER $USERNAME 
WORKDIR /home/rapids

FROM base AS runner
COPY test_file.txt /home/rapids/
ENTRYPOINT [ "/bin/bash", "-c", "nvidia-smi; cat CMakeLists.txt" ]