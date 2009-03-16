#! /bin/bash
# runAll.sh runID exposureList
set -x

pwd = `pwd`
echo "Starting runId "$1
echo `date`

echo "Starting subtraction/detection pipeline"
ssh lsst9 ". /share/stack/dc2pipe/setUpAll.sh; cd /share/stack/dc2pipe/imageSubtractionDetection; nohup ./runPipeline.py -c $1 < /dev/null >imageSubtractionDetection-$1.log 2>&1 &" 

echo "Starting association pipeline"
ssh lsst10 ". /share/stack/dc2pipe/setUpAll.sh; cd /share/stack/dc2pipe/association; nohup sh run.sh policy/pipeline_policy.paf $1 < /dev/null > association-$1.log 2>&1 &"

echo "MovingObjects pipeline starting"
ssh lsst4 ". /share/stack/dc2pipe/setUpAll.sh; cd /share/stack/dc2pipe/movingobjects; nohup sh run.sh policy/pipeline_policy.paf $1 < /dev/null > movingobjects-$1.log 2>&1 &"

echo "Pausing 45 seconds before triggering events"
sleep 45
echo "Sending trigger events"
# cd $pwd
python /share/stack/dc2pipe/imageSubtractionDetection/eventgenerator.py < $2

