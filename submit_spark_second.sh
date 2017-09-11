#!/usr/bin/env bash
sbt assembly && \
../spark-2.2.0-bin-hadoop2.7/bin/spark-submit \
    --class "com.github.plavreshin.ReadingWithSparkAdvanced" \
    --master local \
    --driver-memory 4G \
    target/scala-2.11/spark-playground.jar  \
    ./src/main/resources/task2_sample.csv

