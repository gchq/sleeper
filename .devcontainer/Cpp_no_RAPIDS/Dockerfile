# A C++ Nvidia RAPIDS development image
FROM ubuntu:22.04 AS base

USER root
# Install tools
RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y tzdata htop dstat less nano sudo software-properties-common wget unzip libgrpc++1

# Use shell form for redirection and enable sudo
ARG USERNAME=rapids
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the user
RUN groupadd --gid $USER_GID $USERNAME && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME
RUN echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME
RUN chmod 0440 /etc/sudoers.d/$USERNAME

# Drop to normal user
USER $USERNAME 
WORKDIR /home/rapids

FROM base AS build
USER root
# Install tools
RUN add-apt-repository ppa:ubuntu-toolchain-r/test

# Install Java
RUN wget -O- "https://apt.corretto.aws/corretto.key" | apt-key add -
RUN echo 'deb https://apt.corretto.aws stable main' | tee /etc/apt/sources.list.d/corretto.list

# Install Clang
RUN wget -O "llvm.sh" "https://apt.llvm.org/llvm.sh" && chmod u+x llvm.sh && ./llvm.sh 18

# The above line will run an apt update so now we install Java
RUN apt-get install -y java-17-amazon-corretto-jdk make cmake cmake-format cmake-curses-gui gcc-13 g++-13 clang-format-18 \
    clang-tidy-18 clang-tools-18 maven ccache cppcheck doxygen iwyu \
    build-essential pkg-config protobuf-compiler libgrpc++-dev

# Install aws
RUN wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -O "awscliv2.zip" && unzip awscliv2.zip && ./aws/install

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
ENTRYPOINT [ "/bin/bash", "-c", "cat CMakeLists.txt" ]