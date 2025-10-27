source "../../functions/checkInstalled.sh"

checkInstalled rsync
cd ../../../..
sudo rm -rf temp
mkdir temp
sudo rsync -a --exclude=".*" sleeeper/ temp