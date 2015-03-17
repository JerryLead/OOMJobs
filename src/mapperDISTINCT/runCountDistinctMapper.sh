#!/bin/bash

# variables
reducers=9
io_sort_mb=400
xmx=1500
xms=0

# pig all benchmark queries

DISTINCT_COMBINE="distinct_rankings_map_combine.pig"


mapred_child_java_opts=\'"-Xmx"$xmx"m -Xms"$xmx"m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"\'
echo mapred_child_java_opts=$mapred_child_java_opts > jvm.param

PIG_CMD="$PIG_HOME/bin/pig -param name=$DISTINCT_COMBINE -param_file jvm.param -param io_sort_mb=$io_sort_mb -param reducers=$reducers"

$PIG_CMD $DISTINCT_COMBINE
echo $PIG_CMD $DISTINCT_COMBINE