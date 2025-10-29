##This function checks if something is already installed on the system such as rsync and will install it if not.
##When running an scripts that calls this, do not use the "foo.sh &> bar.log & less bar.log" command as it causes
##  this apt-get to become stuck. Rather use "foo.sh | tee bar.log"
checkInstalled() {
    if [ $(dpkg-query -W -f='${Status}' $1 2>/dev/null | grep -c "ok installed") -eq 0 ]; then
        echo "$1 not installed. Installing now..."
        sudo apt-get update
        sudo apt-get install --yes $1
    else
        echo "$1 already installed."
    fi
}