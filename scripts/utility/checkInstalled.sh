if [ $(dpkg-query -W -f='${Status}' $1 2>/dev/null | grep -c "ok installed") -eq 0 ];
then
  echo "$1 not instlaled. Installing now..."
else
  echo "$1 already installed."
fi