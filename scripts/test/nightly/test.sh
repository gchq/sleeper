
SUITE_PARAMS=("-Dsleeper.system.test.cluster.enabled=true" "-DskipRust" "-Dsleeper.system.test.create.multi.platform=false")

copyFolderForParallelRun() {
    echo "${SUITE_PARAMS[@]}"
}

copyFolderForParallelRun