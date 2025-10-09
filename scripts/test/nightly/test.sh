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