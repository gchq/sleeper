: '
cd ../../..
#Make copies of the java folder to run independent maven builds in parallel
mkdir test
cp -r java test/java
cp -r scripts test/scripts
cp README.md test
cp -r test test2

echo "copied"
ls | grep test

./test/scripts/build/build.sh -DskipRust&
sleep 30; ./test2/scripts/build/build.sh -DskipRust
wait

#Remove the temporary folders
rm -rf test
rm -rf test2

echo "removed"
ls | grep test
'
THIS_DIR=$(cd "$(dirname "$0")" && pwd)
SCRIPTS_DIR=$(cd "$THIS_DIR" && cd ../.. && pwd)
MAVEN_DIR=$(cd "$SCRIPTS_DIR" && cd ../java && pwd)
REPO_PARENT_DIR=$(cd "$SCRIPTS_DIR" && cd ../.. && pwd)

run1() {
    pushd "$SCRIPTS_DIR"
    sleep 2
    echo "1"
    ls
}

run2() {
    pushd "$MAVEN_DIR"
    echo "2"
    ls
}

run1&
run2
wait
