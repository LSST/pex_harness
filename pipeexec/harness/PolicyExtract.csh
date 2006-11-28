#!/bin/tcsh
#echo "LSST_POLICY_DIR: ${LSST_POLICY_DIR}"
#echo "Doing: grep '^stage' ${LSST_POLICY_DIR}/Pipeline.policy"
set line=`echo "$1 1 + p q" | dc`
grep '^stage' ${LSST_POLICY_DIR}/Pipeline.policy | head -${line} | tail -1 | sed -e 's/\n//' -e  's/\t/ /g' -e 's/^ *stage *= *//' -e "s/ *\(.*\) *, *\(.*\) *, *\(.*\) *, *\(.*\) *, *\(.*\)/\$2/" | tr -d "\012"
