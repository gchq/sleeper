checkInstalled() {
    if [ $(dpkg-query -W -f='${Status}' $1 2>/dev/null | grep -c "ok installed") -eq 0 ]; then
        echo "$1 not installed. Installing now..."
        sudo apt-get update
        sudo apt-get install --no $1
        echo "test"
    else
        echo "$1 already installed."
    fi
    return 0
}