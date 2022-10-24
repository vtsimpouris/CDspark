package bounding;

import _aux.Pair;
import _aux.ResultTuple;
import _aux.lib;
import algorithms.AlgorithmEnum;
import clustering.Cluster;
import clustering.ClusteringAlgorithmEnum;
import clustering.HierarchicalClustering;
import core.Main;
import core.Parameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.EuclideanSimilarity;
import similarities.functions.PearsonCorrelation;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class RecursiveBoundingTest {
    private final static Logger LOG = Main.getLogger(Level.INFO);
    private final static String inputPath = "/home/jens/tue/data";
    private final static int n = 50;
    private final static int m = 100;
    private static double[][] data;

    private final static double startEpsilon = 0.79;
    private final static int defaultDesiredClusters = 3;
    private final static double epsilonMultiplier = 0.8;
    private final static int maxLevels = 10;
    private final static ClusteringAlgorithmEnum clusteringAlgorithm = ClusteringAlgorithmEnum.KMEANS;
    private final static int breakFirstKLevelsToMoreClusters = 0;
    private final static int clusteringRetries = 5;
    private static boolean empiricalBounds = true;

    private HierarchicalClustering HC;
    private RecursiveBounding RB;
    private Parameters par;

    private void getParameters(MultivariateSimilarityFunction simMetric){
        par = new Parameters(
                LOG, "", "", false, false, "", 1, AlgorithmEnum.SIMILARITY_DETECTIVE,
                false, false, 1, simMetric, "avg",
                new ArrayList<>(Arrays.asList(new double[]{1})), new ArrayList<>(Arrays.asList(new double[]{1}, new double[]{.5,.5})),
                1, 2, false, "stock", "", new String[n], data, n, m,
                0, empiricalBounds, 0.95, -1, startEpsilon, epsilonMultiplier, maxLevels, defaultDesiredClusters,
                clusteringAlgorithm, breakFirstKLevelsToMoreClusters, clusteringRetries, 1,
                100, 0, -1, ApproximationStrategyEnum.SIMPLE
        );
        par.init();
    }



    @Before
    public void setUp(){
        Pair<String[], double[][]> dataPair = Main.getData("stock", inputPath, n, m, 0, LOG);
        double[][] rawData = dataPair.y;
        MultivariateSimilarityFunction simMetric = new PearsonCorrelation();
        data = simMetric.preprocess(rawData);

        getParameters(simMetric);
        par.setPairwiseDistances(lib.computePairwiseDistances(data, simMetric.distFunc, true));

//        Get cluster tree
        HC = new HierarchicalClustering(par);
        HC.run();
    }

    @Test
    public void testFullRunEmpirical(){
        par.empiricalBounding = true;

        //        Run recursive bounding
        RB = new RecursiveBounding(par, HC.clusterTree);
        List<ResultTuple> results = new ArrayList<>(RB.run());

        List<ResultTuple> expectedResults = Arrays.asList(
                new ResultTuple(new ArrayList<>(Arrays.asList(19)), new ArrayList<>(Arrays.asList(45,14)), new ArrayList<>(), new ArrayList<>(), .95),
                new ResultTuple(new ArrayList<>(Arrays.asList(1)), new ArrayList<>(Arrays.asList(48,18)), new ArrayList<>(), new ArrayList<>(), .95)
        );

        //    Test correct results - empirical bounds
        for (int i = 0; i < results.size(); i++) {
            Assert.assertTrue(expectedResults.contains(results.get(i)));
        }
        for (int i = 0; i < expectedResults.size(); i++) {
            Assert.assertTrue(results.contains(expectedResults.get(i)));
        }

        //    Test number of lookups - empirical bounds
        Assert.assertEquals(14810, par.simMetric.nLookups.get());


        //    Test number of CCs - empirical bounds
        Assert.assertEquals(6726, par.statBag.nCCs.get());

        //    Test number of positive DCCs - empirical bounds
        Assert.assertEquals(18, par.statBag.otherStats.get("nPosDCCs"));

        //    Test number of negative DCCs - empirical bounds
        Assert.assertEquals(0L, par.statBag.otherStats.get("nNegDCCs"));
    }

    @Test
    public void testFullRunTheoretical(){
        par.empiricalBounding = false;

        //        Run recursive bounding
        RB = new RecursiveBounding(par, HC.clusterTree);
        List<ResultTuple> results = new ArrayList<>(RB.run());

        List<ResultTuple> expectedResults = Arrays.asList(
                new ResultTuple(new ArrayList<>(Arrays.asList(19)), new ArrayList<>(Arrays.asList(45,14)), new ArrayList<>(), new ArrayList<>(), .95),
                new ResultTuple(new ArrayList<>(Arrays.asList(1)), new ArrayList<>(Arrays.asList(48,18)), new ArrayList<>(), new ArrayList<>(), .95)
        );

        //    Test correct results - theoretical bounds
        for (int i = 0; i < results.size(); i++) {
            Assert.assertTrue(expectedResults.contains(results.get(i)));
        }
        for (int i = 0; i < expectedResults.size(); i++) {
            Assert.assertTrue(results.contains(expectedResults.get(i)));
        }

        //    Test number of lookups - theoretical bounds
        Assert.assertEquals(0, par.simMetric.nLookups.get());

        //    Test number of positive DCCs - theoretical bounds
        Assert.assertEquals(18, par.statBag.otherStats.get("nPosDCCs"));
    }

//    Test unpackAndCheckMinJump
    @Test
    public void testUnpackAndCheckMinJump() {
        par.empiricalBounding = true;
        par.tau = 0.94;

        Cluster[] allClusters = HC.getAllClusters();
        Cluster C1 = allClusters[35];
        Cluster C2 = allClusters[33];
        Cluster C3 = allClusters[34];

//        Make a positive cluster combination
        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(C1));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(C2,C3));
        ClusterCombination CC = new ClusterCombination(LHS, RHS, 0);
        CC.bound(par.simMetric, true, par.Wl.get(0), par.Wr.get(1), par.getPairwiseDistances());
        List<ClusterCombination> pDCCs = new ArrayList<>(Arrays.asList(CC));

//        CC similarity is 0.956635, max subset similarity is 0.9468.
//        Let's first test with insignificant minjump
        List<ClusterCombination> unpacked = RecursiveBounding.unpackAndCheckMinJump(pDCCs, par);
        Assert.assertTrue(unpacked.contains(CC));

//        Let's now test with significant minjump
        par.minJump = 0.02;
        unpacked = RecursiveBounding.unpackAndCheckMinJump(pDCCs, par);
        Assert.assertFalse(unpacked.contains(CC));

//        Test if CC filtered out when UB != LB
        par.minJump = 0;
        CC.UB = 0.9;
        unpacked = RecursiveBounding.unpackAndCheckMinJump(pDCCs, par);
        Assert.assertFalse(unpacked.contains(CC));
    }

//    Test recursiveBounding function
    @Test
    public void testRecursiveBounding() {
        par.empiricalBounding = true;
        RB = new RecursiveBounding(par, HC.clusterTree);

        Cluster[] allClusters = HC.getAllClusters();
        Cluster C1 = allClusters[35].getParent();
        Cluster C2 = allClusters[33].getParent();
        Cluster C3 = allClusters[34].getParent();

//        Make a big cluster combination
        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(C1));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(C2, C3));
        ClusterCombination CC = new ClusterCombination(LHS, RHS, 0);

//        Bound it
        List<ClusterCombination> DCCs = RB.recursiveBounding(CC, 1, par);
        List<ClusterCombination> pDCCs = DCCs.stream().collect(Collectors.groupingBy(ClusterCombination::isPositive)).get(true);

        //    Test nResults
        Assert.assertEquals(1, DCCs.size());
        Assert.assertEquals(1, pDCCs.size());

        //    Test nCCs
        Assert.assertEquals(19, par.statBag.nCCs.get());

        //    Test nLookups
        Assert.assertEquals(24, par.simMetric.nLookups.get());

        //        Test DCCs actually decisive
        for (ClusterCombination DCC : DCCs) {
            Assert.assertTrue(DCC.isDecisive());
        }
    }
}
