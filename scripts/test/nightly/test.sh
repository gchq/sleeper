THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SLEEPER_DIR=$(cd "$THIS_DIR" && cd ../../.. && pwd)

sudo rm -rf quick
pushd $SLEEPER_DIR
#Make copies of the java folder to run independent maven builds in parallel
sudo rm -rf quick
mkdir quick
cp README.md quick
popd

SUITE=$1

copyFolderForParallelRun() {
    echo "Attempting to copy quick to: $1"
    pushd $SLEEPER_DIR
    sudo rm -rf $1
    sudo cp -r quick $1
    popd
}

copyFolderForParallelRun "$1"



