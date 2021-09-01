#!/bin/bash

CONFIG_FILE=$1

if [ $# -lt 1 ];
then
	echo "USAGE: $0 {configFile}"
	exit 1
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

base_dir=$(dirname $0)/..

export CLASSPATH

PID_FILE='datanode.pid'
MAIN_CLASS='com.ruyuan.dfs.datanode.DataNode'

# 包含lib文件夹中的jar包
for file in "$base_dir"/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done;


JAVA_OPTS="-server"

LOGBACK_OPTS="-Dlogback.configurationFile=conf/logback-datanode.xml"

nohup $JAVA $JAVA_OPTS $LOGBACK_OPTS -cp $CLASSPATH $MAIN_CLASS "$@" > nohup.out 2>&1 &

PID=$!

echo $PID > $(dirname $0)/$PID_FILE
cat $(dirname $0)/$PID_FILE

# echo "查看nohup.out文件是否启动成功"