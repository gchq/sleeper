some(){
    echo $@
}

test=("a" "b" "c")
some "thing" "${test[@]}" $@
test2=("${test[@]}" "d")

echo "${test[@]}"
echo "${test2[@]}"