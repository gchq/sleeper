test() {
    sleep $1
    echo $2
}

blah(){
    test 0 "starting" &
    test 5 "mid" &
    test 10 "end"
}

blah
wait



