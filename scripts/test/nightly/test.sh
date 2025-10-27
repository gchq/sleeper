source "../../functions/checkInstalled.sh"
source "../../functions/timeUtils.sh"

checkInstalled rsync
cd ../../../..
sudo rm -rf temp
mkdir temp
RSYNC_START_TIMESTAMP=$(record_time)
sudo rsync -a --exclude=".*" sleeper/ temp
RSYNC_END_TIMESTAMP=$(record_time)
echo "First one: $RSYNC_START_TIME to $RSYNC_END_TIME"

mkdir temp2
RSYNC2_START_TIMESTAMP=$(record_time)
sudo rsync -a temp temp2
RSYNC2_END_TIMESTAMP=$(record_time)
echo "Second one: $RSYNC2_START_TIME to $RSYNC2_END_TIME"

mkdir temp3
CP_START_TIMESTAMP=$(record_time)
sudo cp -r -p temp temp3
CP_END_TIMESTAMP=$(record_time)
echo "Third one: $CP_START_TIME to $CP_END_TIME"