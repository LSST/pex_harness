#! /bin/bash
#
if [ -z "$CTRL_DC3PIPE_DIR" ]; then
    echo "CTRL_DC3PIPE_DIR environment not set; you need to 'source loadLSST.?'"
    exit 1
fi
DEF_NODES_FILE=$CTRL_DC3PIPE_DIR/etc/ensuressh_nodes.txt
MPDCONF=etc/mpd.conf

NODEFILE=$1
if [ -z "$NODEFILE" ]; then
    NODEFILE=$DEF_NODES_FILE
fi
if [ ! -f "$NODEFILE" -o ! -r "$NODEFILE" ]; then
    echo "$NODEFILE: Not file with read permission"
    exit 1
fi
nodes=`cat $NODEFILE`

for node in $nodes; do 
    echo Updating \$HOME/.mpd.conf on $node
    cat $CTRL_DC3PIPE_DIR/$MPDCONF | ssh $node csh -f -c "'cat >>! $HOME/.mpd.conf'"
    ssh $node chmod 600 .mpd.conf
done
