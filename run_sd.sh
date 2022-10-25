#!/usr/bin/bash

logLevel=FINE
algorithm=SIMILARITY_DETECTIVE
inputPath=/home/data
outputPath=output
simMetricName=PEARSON_CORRELATION
aggPattern=avg
empiricalBounding=true
dataType=stock
n=1000
m=1000000000
partition=0
tau=0.9
minJump=0.05
maxPLeft=1
maxPRight=2
allowSideOverlap=false
shrinkFactor=1
topK=-1
approximationStrategy=SIMPLE
seed=0
parallel=true
random=false
saveStats=true
saveResults=false

java \
-Djava.util.concurrent.ForkJoinPool.common.parallelism=46 \
-cp target/SimilarityDetective-1.0-jar-with-dependencies.jar \
core/Main \
$logLevel \
$algorithm \
$inputPath \
$outputPath \
$simMetricName \
$aggPattern \
$empiricalBounding \
$dataType \
$n \
$m \
$partition \
$tau \
$minJump \
$maxPLeft \
$maxPRight \
$allowSideOverlap \
$shrinkFactor \
$topK \
$approximationStrategy \
$seed \
$parallel \
$random \
$saveStats \
$saveResults \
