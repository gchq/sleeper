echo "some" "thing" 5
echo "75"
TEST_OUTPUT_DIR="blah"
echo mvn --batch-mode site site:stage -pl system-test/system-test-suite \
       -DskipTests=true \
       -DstagingDirectory="$TEST_OUTPUT_DIR/site"