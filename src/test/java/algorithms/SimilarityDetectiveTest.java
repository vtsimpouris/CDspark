package algorithms;

import _aux.ResultTuple;
import _aux.lib;
import algorithms.performance.SimilarityDetective;
import bounding.ApproximationStrategyEnum;
import clustering.ClusteringAlgorithmEnum;
import core.Main;
import core.Parameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.PearsonCorrelation;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SimilarityDetectiveTest {
    Logger LOG = Main.getLogger(Level.INFO);
    AlgorithmEnum algorithm = AlgorithmEnum.SIMILARITY_DETECTIVE;
    String inputPath = "/home/jens/tue/data";
    String outputPath = "output";
    MultivariateSimilarityFunction simMetric = new PearsonCorrelation();
    String aggPattern = "avg";
    boolean empiricalBounding = true;
    String dataType = "stock";
    int n = 100;
    int m = (int) 1e7;
    int partition = 0;
    double tau = 0.90;
    double minJump = 0.05;
    int maxPLeft = 1;
    int maxPRight = 2;
    boolean allowSideOverlap = false;
    int shrinkFactor = 1;
    int topK = -1;
    ApproximationStrategyEnum approximationStrategy = ApproximationStrategyEnum.SIMPLE;
    int seed = 0;
    boolean parallel = false;
    boolean random = false;
    boolean saveStats = false;
    boolean saveResults = true;
    int defaultDesiredClusters = 10;
    double startEpsilon = simMetric.simToDist(0.81*simMetric.MAX_SIMILARITY);
    double maxApproximationSize = simMetric.simToDist(0.9*simMetric.MAX_SIMILARITY);
    double epsilonMultiplier = 0.8;
    int maxLevels = 20;
    ClusteringAlgorithmEnum clusteringAlgorithm = ClusteringAlgorithmEnum.KMEANS;
    int breakFirstKLevelsToMoreClusters = 0;
    int clusteringRetries = 50;
    int nPriorityBuckets = 50;
    List<double[]> Wl = List.of(new double[]{1});
    List<double[]> Wr = List.of(new double[]{1}, new double[]{.5,.5});

    String resultPath = String.format("%s/results/%s_n%s_m%s_part%s_tau%.2f.csv", outputPath, dataType,
            n, m, partition, tau);
    String dateTime = (new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")).format(new Date());
    int threads = ForkJoinPool.getCommonPoolParallelism();
    double[][] data = lib.l2norm(Main.getData(dataType, inputPath, n, m, partition, LOG).y);

    Parameters par = new Parameters(LOG, dateTime, "test", saveStats, saveResults, resultPath, threads,
            algorithm, parallel, random, seed, simMetric, aggPattern, Wl, Wr, maxPLeft, maxPRight, allowSideOverlap,
            dataType, outputPath, new String[n], data, n, m, partition, empiricalBounding, tau, minJump, startEpsilon,
            epsilonMultiplier, maxLevels, defaultDesiredClusters, clusteringAlgorithm, breakFirstKLevelsToMoreClusters,
            clusteringRetries, shrinkFactor, maxApproximationSize, nPriorityBuckets, topK, approximationStrategy);

    private List<ResultTuple> getResultsFromFile(String filename) {
        List<ResultTuple> results = new ArrayList<>();
        List<String> lines = lib.readCSV(filename);

//        Skip header
        for (int i=1; i<lines.size(); i++) {
            String[] split = lines.get(i).split(",");
            List<Integer> LHS = Arrays.stream(split[0].split("-")).map(Integer::parseInt).collect(Collectors.toList());
            List<Integer> RHS = Arrays.stream(split[1].split("-")).map(Integer::parseInt).collect(Collectors.toList());
            double sim = Double.parseDouble(split[4]);
            results.add(new ResultTuple(LHS,RHS, new ArrayList<>(), new ArrayList<>(), sim));
        }
        return results;
    }

    @Before
    public void setUp() {
        par.init();
    }

    //    Test threshold query (all above threshold, correct results read from file)
    @Test
    public void testThresholdQuery(){
        Algorithm sd = new SimilarityDetective(par);
        List<ResultTuple> results = new ArrayList<>(sd.run());
        List<ResultTuple> expectedResultList = getResultsFromFile("src/test/resources/sd_threshold_results.csv");

        Assert.assertEquals(expectedResultList.size(), results.size());
        for (int i = 0; i < expectedResultList.size(); i++) {
            ResultTuple expected = expectedResultList.get(i);
            boolean inResults = results.contains(expected);
            if (!inResults){
                LOG.info(String.format("Expected result %s not found in results", expected));
            }
            Assert.assertTrue(inResults);
        }

//        Also do left join on results to check if there are any unexpected results
        for (int i = 0; i < results.size(); i++) {
            ResultTuple result = results.get(i);
            boolean inExpected = expectedResultList.contains(result);
            if (!inExpected){
                LOG.info(String.format("Unexpected result %s found in results", result));
            }
            Assert.assertTrue(inExpected);
        }
    }

//    Test topk (only k results, correct results read from file)
    @Test
    public void testTopKQuery(){
        par.topK = 10;
        par.shrinkFactor = 0;
        par.tau = 0.5;

        Algorithm sd = new SimilarityDetective(par);
        List<ResultTuple> results = new ArrayList<>(sd.run());

        List<ResultTuple> expectedResultList = getResultsFromFile("src/test/resources/sd_topk_results.csv");

        Assert.assertEquals(expectedResultList.size(), results.size());
        for (int i = 0; i < expectedResultList.size(); i++) {
            ResultTuple expected = expectedResultList.get(i);
            boolean inResults = results.contains(expected);
            if (!inResults){
                LOG.info(String.format("Expected result %s not found in results", expected));
            }
            Assert.assertTrue(inResults);
        }

//        Also do left join on results to check if there are any unexpected results
        for (int i = 0; i < results.size(); i++) {
            ResultTuple result = results.get(i);
            boolean inExpected = expectedResultList.contains(result);
            if (!inExpected){
                LOG.info(String.format("Unexpected result %s found in results", result));
            }
            Assert.assertTrue(inExpected);
        }
    }
}
