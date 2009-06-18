#! /bin/bash
#
#set -x

# source the setup script that is the argument of -S 
count=0
flag1=0
for i in $@
do
   if [ $flag1 -eq 1 ]
   then
       setupScript=$i
       source $setupScript
   fi
   if [ $i == "-S" ]
   then
       echo "match" $i 
       flag1=1
   else
       flag1=0
   fi
   let "count++"
done


# gather the arguments other than "-S setup.sh" 
# into 'newargs'
count2=0
newargs=""
for j in $@
do
   let countm=count-2
   if [ $count2 -lt $countm ]
   then
       newargs="$newargs $j"
   fi
   let "count2++"
done

# modify the nodelist.scr based on the PBS_NODEFILE
nodeFile="nodelist.scr"
pbsFile=$PBS_NODEFILE

# Read the raw nodelist.scr line by line into array   nodeArray
nodeLine=""
ncount=0
while [ 1 ]
do
    read nodeLine || break
    nodeArray[$ncount]=$nodeLine
    # echo nodeArray $ncount $nodeLine ${nodeArray[ncount]}
    let "ncount++"
done < $nodeFile 

# Read the Abe PBS_NODEFILE line by line into array   pbsArray
pbsLine=""
pcount=0
while [ 1 ]
do
    read pbsLine || break
    pbsArray[$pcount]=$pbsLine
    # echo pbsArray $pcount $pbsLine ${pbsArray[pcount]}
    let "pcount++"
done < $pbsFile 

# Loop through the nodeArray  and extract the number of processes after the : 
icount=0
while [ $icount -lt $pcount ]
do
    anodeLine=${nodeArray[icount]}
    # echo icount $icount $pcount $anodeLine 
    
    lineArray=($(echo $anodeLine | awk -F"." '{for(ij=1;ij<=NF;ij++) print $ij}'))
    # echo "num lineArray", ${#lineArray[@]}

    ii=0
    while [ $ii -lt ${#lineArray[@]} ]
    do
        # echo lineArray $ii ${lineArray[ii]}
        let index1=${#lineArray[@]}-1
        # echo lineArray2 $index1
        if [ $ii -eq ${index1} ]
        then 
           lastItem=${lineArray[ii]}
        fi 
        let "ii++"
    done

    # echo lastItem ${lastItem}
    lastArray=($(echo $lastItem | awk -F":" '{for(ij=1;ij<=NF;ij++) print $ij}'))
    # echo lastArray ${lastArray[0]} ${lastArray[1]}
    countArray[$icount]=${lastArray[1]}

    let "icount++"
done

# Over nodelist.scr with the proper contents
# This consists of taking the PBS nodefile lines and appending : and the number of processes.
overwriteNodeFile="nodelist.scr"
icount=0
while [ $icount -lt $pcount ]
do
    str=""
    str="$str${pbsArray[icount]}"
    # str="$str${pbsArray[icount]}-ib0"
    # str="$str.${lineArray[1]}.${lineArray[2]}.${lineArray[3]}"
    str="$str:${countArray[icount]}"
    if [ $icount -eq 0 ] 
    then 
        echo $str  > $overwriteNodeFile
    else 
        echo $str  >> $overwriteNodeFile
    fi
    let "icount++"
done  

PYTHONPATH=/u/ac/daues/lsst-abe/add_ons/harness-3.3.2/examples/stages:${PYTHONPATH}
echo PYTHONPATH $PYTHONPATH

bindir=""
if echo $0 | grep -q /; then
    bindir=`dirname $0`
fi

if [ -z "$PEX_HARNESS_DIR" ]; then

    # make sure we load the ctrl_dc3pipe environment
    version=`dirname "$bindir"`         # pex_harness version directory
    if [ "$version" = "."  ]; then
        echo "Unable to load pex_harness: unable to determine the version from $bindir"
        exit 1
    fi

    if [ -z "$LSST_HOME" ]; then
        home=`dirname "$version"`           # pex_harness directory
        home=`dirname "$home"`              # flavor directory
        export LSST_HOME=`dirname "$home"`  # LSST_HOME directory

    fi
    version=`basename $version`

    if [ ! -f "$LSST_HOME/loadLSST.sh" ]; then
        echo "Unable to load LSST stack; " \
             "can't find $LSST_HOME/loadLSST.sh"
        exit 1
    fi

    SHELL=/bin/bash
    source  $LSST_HOME/loadLSST.sh
    setup pex_harness $version
fi

eups list 2> /dev/null | grep Setup > eups-env.txt

pipeline=`echo ${1} | sed -e 's/\..*$//'`


# nohup $bindir/launchPipeline.py $* > ${pipeline}-${2}.log 2>&1  &
# nohup launchPipeline.py $* > ${pipeline}-${2}.log 2>&1  &
# launchPipeline.py $newargs > ${pipeline}-${2}.log 2>&1  &
# echo TERM $TERM
# /usr/bin/env | grep PBS 
# echo SHELL $SHELL
echo PBS_NODEFILE ${PBS_NODEFILE}
cat ${PBS_NODEFILE}

# launchPipeline.py $newargs -n ${PBS_NODEFILE}
launchPipeline.py $newargs

