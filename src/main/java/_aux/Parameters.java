package _aux;

import algorithms.AlgorithmEnum;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import similarities.functions.SimEnum;

import java.util.logging.Logger;

@RequiredArgsConstructor
public class Parameters {
//    Logging
    @NonNull public Logger LOGGER;
    @NonNull public String dateTime;
    @NonNull public String codeVersion;
    @NonNull public boolean saveStats;
    @NonNull public boolean saveResults;
    @NonNull public String resultPath;
    @NonNull public int threads;

    //    Run details
    @NonNull public AlgorithmEnum algorithm;
    @NonNull public boolean parallel;
    @NonNull public boolean random;
    @NonNull public int run;

    //    Query
    @NonNull public SimEnum simMetric;
    @NonNull public int maxPLeft;
    @NonNull public int maxPRight;

    //    Data
    @NonNull public String dataType;
    @NonNull public String outputPath;
    @NonNull public String[] headers;
    @NonNull public double[][] data;
    @NonNull public int n;
    @NonNull public int m;
    @NonNull public int partition;

    //    Bounding
    @NonNull boolean empiricalBounding;

    //    Clustering
    @NonNull public double tau;
    @NonNull public double minJump;
    @NonNull public double startEpsilon;
    @NonNull public double epsilonMultiplier;
    @NonNull public int maxLevels;
    @NonNull public int defaultDesiredClusters;
    @NonNull public boolean useKMeans;
    @NonNull public int breakFirstKLevelsToMoreClusters;
    @NonNull public int clusteringRetries;

    //    Top-k
    @NonNull public double shrinkFactor;
    @NonNull public double maxApproximationSize;
    @NonNull public int nPriorityBuckets;
    @NonNull public int k;
    @NonNull public String approximationStrategy;
}
