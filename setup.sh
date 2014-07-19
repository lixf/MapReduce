######################################################################
## Xiaofan Li
## 15440 
## setup script
## I don't know Makefiles for Java, especially I don't like classpath
## because it's confusing. Please run this script to compile and run
######################################################################


#!/bin/bash
echo ""
echo "please make sure you use bash"
echo "current shell is: "$SHELL
echo ""

## fonts
blue='\e[0;34m'
green='\e[0;32m'
red='\e[0;31m'
NC='\e[0m' # No Color
echo -e "${blue}Hello User${NC}"
echo -e "${blue}Welcome to my mapReduce control${NC}"

## paths
SOURCE_PATH='/afs/andrew.cmu.edu/usr9/xli2/private/15440/p3/MapReduce/source'
BIN_PATH='/afs/andrew.cmu.edu/usr9/xli2/private/15440/p3/MapReduce/bin'
NAME_NODE_SOURCE=$SOURCE_PATH'/server/name_node/*.java'
DATA_NODE_SOURCE=$SOURCE_PATH'/server/data_node/*.java'
CLIENT_SOURCE=$SOURCE_PATH'/client/*.java'
TOOLS_SOURCE=$SOURCE_PATH'/tools'
BINARY_SOURCE=$BIN_PATH

## define some functions
function makefiles {
    # Compile all source code
    javac -Xlint -d $BINARY_SOURCE $TOOLS_SOURCE/*.java
    javac -Xlint -d $BINARY_SOURCE -cp $TOOLS_SOURCE $NAME_NODE_SOURCE
    javac -Xlint -d $BINARY_SOURCE -cp $TOOLS_SOURCE $DATA_NODE_SOURCE
    javac -Xlint -d $BINARY_SOURCE -cp $TOOLS_SOURCE $CLIENT_SOURCE
    echo -e "${green}all files compiled in ./bin/${NC}"
}

function cleanfiles {
    #delete all files 
    rm ./bin/*
    echo -e "${green}all class files removed${NC}"
}

function rundatanode {
    #run data node blocks
    echo -e "${green}starting data node server${NC}"
    cd $BINARY_SOURCE
    java JavaDataNode
}

function runnamenode {
    #run name node blocks
    echo -e "${green}starting name node server${NC}"
    cd $BINARY_SOURCE
    java JavaNameNode
}

function runclient {
    #run client with command prompt
    echo -e "${green}starting client${NC}"
    echo -e "${green}please choose from sum/search :)${NC}"
    read choice
    cd $BINARY_SOURCE
    case "$choice" in
    sum)    java JavaClientSum; exit 0;;
    search)    java JavaClientSearch; exit 0;;
    *)      echo ${red}please choose from sum/search${NC}; exit 0;;
    esac
}

function usage {
  echo -e "${red}options: make clean data name client${NC}"
}

## start the program
if [ $# -lt 1 ]; then 
  echo -e "${red}you should at least have one argument!${NC}"
  echo -e "${red}options: make clean server client${NC}"
else 
  while [ "$1" != "" ]; do
    case $1 in
    make  )     makefiles;  exit 0;; 
    clean )     cleanfiles; exit 0;;
    name  )     runnamenode;exit 0;;
    data  )     rundatanode;exit 0;;
    client)     runclient;  exit 0;;
        * )     usage;      exit 0;;
    esac
  done
fi


