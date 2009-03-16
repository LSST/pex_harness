#! /bin/bash

export LSST_DB_AUTH=test:globular.test
echo "Setting up /share/stack on " `hostname`
pwd=`pwd`
cd /share/stack
export LSST_HOME=`pwd`
. ./loadLSST.sh
for i in imageproc movingobj events detection associate dps mwi fw events; do
  setup $i;
done
cd $pwd

