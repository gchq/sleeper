#!/usr/bin/env bash
set -ex

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <version-number> <output-jars-directory> <maven-options>"
  exit 1
fi

SLEEPER_VERSION=$1
JARS_DIR=$2
shift 2

mvn dependency:get -Dartifact="sleeper:clients:$SLEEPER_VERSION:jar:utility" -Dtransitive=false "$@"
mvn dependency:copy -Dartifact="sleeper:clients:$SLEEPER_VERSION:jar:utility" -DoutputDirectory="$JARS_DIR" "$@"

get_jar() {
    local parts=$1
    local artifactId=$(echo "$parts" | cut -d':' -f1)
    local classifier=$(echo "$parts" | cut -d':' -f2)
    local filename=$(echo "$parts" | cut -d':' -f3)
    shift 1
    set -x
    mvn dependency:get -Dartifact="sleeper:$artifactId:$SLEEPER_VERSION:jar:$classifier" -Dtransitive=false "$@"
    mvn dependency:copy -Dartifact="sleeper:$artifactId:$SLEEPER_VERSION:jar:$classifier" -DoutputDirectory="$JARS_DIR" "$@"
    set +x
    local mvnPath="$JARS_DIR/$artifactId-$SLEEPER_VERSION-$classifier.jar"
    local outPath="$JARS_DIR/$filename"
    if [[ "$mvnPath" != "$outPath" ]]; then
        mv -f "$mvnPath" "$outPath"
    fi
}

set +x
java -cp "$JARS_DIR/clients-$SLEEPER_VERSION-utility.jar" \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    sleeper.clients.deploy.jar.ListJars \
    | while read -r line
do
    get_jar $line "$@"
done
