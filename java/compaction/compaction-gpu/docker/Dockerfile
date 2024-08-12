# A C++ Nvidia RAPIDS development image
FROM nvcr.io/nvidia/rapidsai/base:24.08-cuda12.5-py3.11 AS base

USER root
# Install tools
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y tzdata htop dstat less nano sudo software-properties-common wget unzip

# Use shell form for redirection and enable sudo
ARG USERNAME=rapids
RUN echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME
RUN chmod 0440 /etc/sudoers.d/$USERNAME

# Drop to normal user
USER $USERNAME 
WORKDIR /home/rapids

FROM base AS build
USER root
# Install tools
RUN add-apt-repository ppa:ubuntu-toolchain-r/test

# Install aws
RUN wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -O "awscliv2.zip" && unzip awscliv2.zip && ./aws/install

# Install Java
RUN wget -O- "https://apt.corretto.aws/corretto.key" | apt-key add -
RUN echo 'deb https://apt.corretto.aws stable main' | tee /etc/apt/sources.list.d/corretto.list

# Install Clang
RUN wget -O "llvm.sh" "https://apt.llvm.org/llvm.sh" && chmod u+x llvm.sh && ./llvm.sh 18

# The above line will run an apt update so now we install Java
RUN apt-get install -y java-17-amazon-corretto-jdk make cmake-format gcc-13 g++-13 clang-format-18 \
    clang-tidy-18 clang-tools-18 maven ccache cppcheck doxygen iwyu \
    build-essential pkg-config

# Drop to normal user
USER $USERNAME 
WORKDIR /home/rapids

# Install CMake
RUN wget "https://github.com/Kitware/CMake/releases/download/v3.30.1/cmake-3.30.1-linux-x86_64.tar.gz" \
    && mkdir -p cmake \
    && tar --strip-components 1 -C cmake -zxf cmake-3.30.1-linux-x86_64.tar.gz \
    && echo "export PATH=\$PATH:/home/$USERNAME/cmake/bin" >>"/home/$USERNAME/.bashrc"

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