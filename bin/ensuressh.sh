#! /bin/bash
#
if [ -z "$CTRL_DC3PIPE_DIR" ]; then
    echo "CTRL_DC3PIPE_DIR environment not set; you need to 'source loadLSST.?'"
    exit 1
fi
DEF_NODES_FILE=$CTRL_DC3PIPE_DIR/etc/ensuressh_nodes.txt
KNOWN_HOSTS_FILE=$CTRL_DC3PIPE_DIR/etc/ensuressh_known_hosts

NODEFILE=$1
if [ -z "$NODEFILE" ]; then
    NODEFILE=$DEF_NODES_FILE
fi
if [ ! -f "$NODEFILE" -o ! -r "$NODEFILE" ]; then
    echo "$NODEFILE: Not file with read permission"
    exit 1
fi
nodes=`cat $NODEFILE`

cat <<EOF
This script will set attempt to set up SSH keys and related files on all 
nodes in the node list:

EOF
for node in $nodes; do
    echo "   $node"
done
echo
read -p "Proceed? (yes/no/Ctl-C to quit): " okay
if [ "$okay" != "yes" ]; then
    echo "Cancelling script."
    exit 1
fi
echo 

mkdir -p $HOME/.ssh
if [ -f $HOME/.ssh/known_hosts ]; then
   cat $KNOWN_HOSTS_FILE >> $HOME/.ssh/known_hosts
else
   cp $KNOWN_HOSTS_FILE $HOME/.ssh/known_hosts
fi
thishost=`hostname`
echo

for node in $nodes; do 
    if [ "$thishost" != "$node" ]; then
        echo Transfering node list to $node
        cat $KNOWN_HOSTS_FILE | ssh $node csh -f -c \
            "'mkdir -p $HOME/.ssh; cat >>! $HOME/.ssh/known_hosts'"
    fi
done







