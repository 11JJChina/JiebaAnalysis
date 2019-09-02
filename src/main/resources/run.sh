#!/usr/bin/env bash
#在cdh上运行spark任务脚本
spark2-submit \
    --class com.zhang.jieba.JiebaKry \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    --driver-memory 2g \
    --driver-core 1 \
    --executor-memory 2g \
    --executor-cores 3 \
    /home/JiebaAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar
