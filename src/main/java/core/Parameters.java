package core;

import _aux.StatBag;
import algorithms.AlgorithmEnum;
import bounding.ApproximationStrategyEnum;
import clustering.ClusteringAlgorithmEnum;
import lombok.Getter;
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
    @NonNull @Getter public Logger LOGGER;
    @NonNull @Getter public String dateTime;
    @NonNull @Getter public String codeVersion;
    @NonNull @Getter public boolean saveStats;
    @NonNull @Getter public boolean saveResults;
    @NonNull @Getter public String resultPath;
    @NonNull @Getter public int threads;

    //    Run details
    @NonNull @Getter public AlgorithmEnum algorithm;
    @NonNull @Getter public boolean parallel;
    @NonNull @Getter public boolean random;
    @NonNull @Getter public int seed;

    //    Query
    @NonNull @Getter public MultivariateSimilarityFunction simMetric;
    @NonNull @Getter public String aggPattern;
    @NonNull @Getter public List<double[]> Wl;
    @NonNull @Getter public List<double[]> Wr;
    @NonNull @Getter public int maxPLeft;
    @NonNull @Getter public int maxPRight;
    @NonNull @Getter public boolean allowSideOverlap;

    //    Data
    @NonNull @Getter public String dataType;
    @NonNull @Getter public String outputPath;
    @NonNull @Getter public String[] headers;
    @NonNull @Getter public double[][] data;
    @NonNull @Getter public int n;
    @NonNull @Getter public int m;
    @NonNull @Getter public int partition;

    //    Bounding
    @NonNull @Getter public boolean empiricalBounding;

    //    Clustering
    @NonNull @Getter public double tau;
    @NonNull @Getter public double minJump;
    @NonNull @Getter public double startEpsilon;
    @NonNull @Getter public double epsilonMultiplier;
    @NonNull @Getter public int maxLevels;
    @NonNull @Getter public int defaultDesiredClusters;
    @NonNull @Getter public ClusteringAlgorithmEnum clusteringAlgorithm;
    @NonNull @Getter public int breakFirstKLevelsToMoreClusters;
    @NonNull @Getter public int clusteringRetries;

    //    Top-k
    @NonNull @Getter public double shrinkFactor;
    @NonNull @Getter public double maxApproximationSize;
    @NonNull @Getter public int nPriorityBuckets;
    @NonNull @Getter public int topK;
    @NonNull @Getter public ApproximationStrategyEnum approximationStrategy;

//    Misc
    @Getter public StatBag statBag = new StatBag();
    @Getter public Random randomGenerator;
    @Setter @Getter public double[][] pairwiseDistances;


    public void init(){
        randomGenerator = random ? new Random(): new Random(seed);
    }


}

