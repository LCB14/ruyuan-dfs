#!/bin/bash

CONFIG_FILE=$1
BACKUP_OPTION=$2

base_dir=$(dirname $0)/..

if [ $# -lt 1 ];
then
  echo "USAGE: $0 {configFile} [--backup]"
  exit 1
fi

if [[ -n $BACKUP_OPTION && $BACKUP_OPTION == '--backup' ]]
then
  echo "以BackupNode角色启动...."
  MAIN_CLASS='com.ruyuan.dfs.backup.BackupNode'
  PID_FILE='backupnode.pid'
  LOGBACK_OPTS="-Dlogback.configurationFile=conf/logback-backupnode.xml"
else
  echo "以NameNode角色启动...."
  MAIN_CLASS='com.ruyuan.dfs.namenode.NameNode'
  PID_FILE='namenode.pid'
  LOGBACK_OPTS="-Dlogback.configurationFile=conf/logback-namenode.xml"
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

export CLASSPATH

# 包含lib文件夹中的jar包
for file in "$base_dir"/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done;

JAVA_OPTS="-server"

nohup $JAVA $JAVA_OPTS $LOGBACK_OPTS -cp $CLASSPATH $MAIN_CLASS "$@" > nohup.out 2>&1 &

PID=$!

echo $PID > $(dirname $0)/$PID_FILE
cat $(dirname $0)/$PID_FILE

# echo "查看nohup.out文件是否启动成功"