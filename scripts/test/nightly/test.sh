source "../../functions/checkInstalled.sh"
source "../../functions/timeUtils.sh"
source "../../functions/systemTestUtils.sh"

checkInstalled rsync

cd ../../../..
sudo rm -rf temp
mkdir temp
echo "First start: $(time_str)"
sudo rsync -a --exclude=".*" sleeper/ temp
echo "First end: $(time_str)"

sudo rm -rf temp2
mkdir temp2
echo "Second start: $(time_str)"
sudo rsync -a temp temp2
echo "Second end: $(time_str)"

sudo rm -rf temp3
mkdir temp3
echo "Third start: $(time_str)"
sudo cp -r -p temp temp3
echo "Third end: $(time_str)"