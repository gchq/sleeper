source "../../functions/checkInstalled.sh"
source "../../functions/timeUtils.sh"

START_TIMESTAMP=$(record_time)
START_TIME=$(recorded_time_str "$START_TIMESTAMP" "%Y%m%d-%H%M%S")
START_TIME_SHORT=$(recorded_time_str "$START_TIMESTAMP" "%m%d%H%M")

checkInstalled rsync
cd ../../../..
sudo rm -rf temp
mkdir temp
RSYNC_START_TIMESTAMP=$(time_str)
sudo rsync -a --exclude=".*" sleeper/ temp
RSYNC_END_TIMESTAMP=$(time_str)
echo "First one: [$RSYNC_START_TIME] to [$RSYNC_END_TIME]"

sudo rm -rf temp2
mkdir temp2
RSYNC2_START_TIMESTAMP=$(time_str)
sudo rsync -a temp temp2
RSYNC2_END_TIMESTAMP=$(time_str)
echo "Second one: [$RSYNC2_START_TIME] to [$RSYNC2_END_TIME]"

sudo rm -rf temp3
mkdir temp3
CP_START_TIMESTAMP=$(time_str)
sudo cp -r -p temp temp3
CP_END_TIMESTAMP=$(time_str)
echo "Third one: [$CP_START_TIME] to [$CP_END_TIME]"