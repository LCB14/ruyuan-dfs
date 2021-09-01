#!/bin/bash

if [ $# -lt 1 ];
then
	echo "USAGE: $0 dfs.sh 参数"
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

# 包含lib文件夹中的jar包
for file in "$base_dir"/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done;

LOGBACK_OPTS="-Dlogback.configurationFile=conf/logback-client.xml"

$JAVA $JAVA_OPTS $LOGBACK_OPTS -cp $CLASSPATH com.ruyuan.dfs.client.tools.DfsCommand "$@"