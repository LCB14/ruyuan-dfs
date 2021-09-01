#!/bin/bash

CUR_DIR=$(dirname $0)

PID_FILE='datanode.pid'

PID_FILE=$CUR_DIR/$PID_FILE

# -f 参数判断 $file 是否存在
if [ ! -f "$PID_FILE" ]; then
 echo "[OK]"
 exit 0
fi

PID=`cat $PID_FILE`

echo -n "stopping $PID_FILE ...($PID)"

kill -9 $PID

if [ $? -eq 0 ]; then
   echo "[OK]"
else
   echo "[FAILED]"
fi

rm -f $PID_FILE >/dev/null 2>&1