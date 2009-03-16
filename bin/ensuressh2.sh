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
cat <<EOF

This will be done by first creating the SSH keys locally and then transfering 
them to each of the machines.  This will involve logging into each machine.
Each time, you will likely be asked for a password; enter your password at 
that time.  Subsequently, you may type your password many times, once for 
each host.  When this script is complete, you will be set up with password-less 
logins.

EOF

read -p "Are you ready (yes/no/Ctl-C to quit): " okay
if [ "$okay" != "yes" ]; then
    echo "Cancelling script."
    exit 1
fi
echo 

if [ ! -e $HOME/.ssh/id_dsa ]; then 
    echo "Generating SSH keys; IMPORTANT: Do not enter a passphrase; just hit RETURN"
    ssh-keygen -t dsa
fi
if [ ! -f "$HOME/.ssh/id_dsa.pub" ]; then
    echo "No public key generated or found: ~/.ssh/id_dsa.pub"
    exit 1
fi

if [ -f $HOME/.ssh/authorized_keys ]; then
   who=`sed -e 's/^.* //' $HOME/.ssh/id_dsa.pub`
   if grep -q " $who" $HOME/.ssh/authorized_keys; then
       echo Okay--Current node already has keys and authorization set up.
   else
       cat $HOME/.ssh/id_dsa.pub >> $HOME/.ssh/authorized_keys
   fi
else
   cp $HOME/.ssh/id_dsa.pub $HOME/.ssh/authorized_keys
fi
if [ -f $HOME/.ssh/known_hosts ]; then
   cat $KNOWN_HOSTS_FILE >> $HOME/.ssh/known_hosts
else
   cp $KNOWN_HOSTS_FILE $HOME/.ssh/known_hosts
fi
thishost=`hostname`
echo

for node in $nodes; do 
    if [ "$thishost" != "$node" ]; then
        echo Transfering keys to $node
        echo
        cat $HOME/.ssh/id_dsa.pub | \
              ssh $node csh -f -c "'mkdir -p .ssh; cat >>! .ssh/authorized_keys'"
        if [ "$?" != 0 ]; then
            echo "Failed to transfer public key to $node"
            exit 1
        fi

        echo "(Now it should not be necessary to type your password to $node)"
        scp $HOME/.ssh/id_dsa $HOME/.ssh/id_dsa.pub ${node}:.ssh
        cat $KNOWN_HOSTS_FILE | ssh $node csh -f -c \
            "'cat >>! $HOME/.ssh/known_hosts'"
    fi
done







