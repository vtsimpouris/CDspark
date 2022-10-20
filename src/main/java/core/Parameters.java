package core;

import _aux.StatBag;
import algorithms.AlgorithmEnum;
import clustering.ClusteringAlgorithmEnum;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import similarities.MultivariateSimilarityFunction;

import java.util.List;
import java.util.Random;
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
    @NonNull public int seed;

    //    Query
    @NonNull public MultivariateSimilarityFunction simMetric;
    @NonNull public String aggPattern;
    @NonNull public List<double[]> Wl;
    @NonNull public List<double[]> Wr;
    @NonNull public int maxPLeft;
    @NonNull public int maxPRight;
    @NonNull public boolean allowSideOverlap;

    //    Data
    @NonNull public String dataType;
    @NonNull public String outputPath;
    @NonNull public String[] headers;
    @NonNull public double[][] data;
    @NonNull public int n;
    @NonNull public int m;
    @NonNull public int partition;

    //    Bounding
    @NonNull public boolean empiricalBounding;

    //    Clustering
    @NonNull public double tau;
    @NonNull public double minJump;
    @NonNull public double startEpsilon;
    @NonNull public double epsilonMultiplier;
    @NonNull public int maxLevels;
    @NonNull public int defaultDesiredClusters;
    @NonNull public ClusteringAlgorithmEnum clusteringAlgorithm;
    @NonNull public int breakFirstKLevelsToMoreClusters;
    @NonNull public int clusteringRetries;

    //    Top-k
    @NonNull public double shrinkFactor;
    @NonNull public double maxApproximationSize;
    @NonNull public int nPriorityBuckets;
    @NonNull public int k;
    @NonNull public String approximationStrategy;

//    Misc
    public StatBag statBag = new StatBag();
    public Random randomGenerator;
    @Setter public double[][] pairwiseDistances;


    public void init(){
        randomGenerator = random ? new Random(): new Random(seed);
    }


}

