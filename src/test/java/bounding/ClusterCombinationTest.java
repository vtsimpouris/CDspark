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
import similarities.functions.PearsonCorrelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class ClusterCombinationTest {
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
    private Parameters par;

    private void getParameters(MultivariateSimilarityFunction simMetric){
        par = new Parameters(
                LOG, "", "", false, false, "", 1, AlgorithmEnum.SIMILARITY_DETECTIVE,
                false, false, 1, simMetric, "avg",
                new ArrayList<>(Arrays.asList(new double[]{1})), new ArrayList<>(Arrays.asList(new double[]{1}, new double[]{.5,.5})),
                1, 2, false, "stock", "", new String[n], data, n, m,
                0, empiricalBounds, 0.95, -1, startEpsilon, epsilonMultiplier, maxLevels, defaultDesiredClusters,
                clusteringAlgorithm, breakFirstKLevelsToMoreClusters, clusteringRetries, 0, 0, 0, -1, ""
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
    }


    @Test
    public void testSimpleMethods(){
        Cluster C1 = new Cluster(par.simMetric.distFunc, 0);
        Cluster C2 = new Cluster(par.simMetric.distFunc, 5); C2.setId(1);
        Cluster C3 = new Cluster(par.simMetric.distFunc, 10); C3.setId(2);

        for (int i = 1; i < 5; i++) {
            C1.addPoint(i);
            C2.addPoint(i+5);
            C3.addPoint(i+10);

//        Make singleton subclusters
            Cluster c1 = new Cluster(par.simMetric.distFunc, i); c1.finalize(data); C1.addChild(c1); c1.setId(i);
            Cluster c2 = new Cluster(par.simMetric.distFunc, i+5); c2.finalize(data); C2.addChild(c2); c2.setId(i+5);
            Cluster c3 = new Cluster(par.simMetric.distFunc, i+10); c3.finalize(data); C3.addChild(c3); c3.setId(i+10);
        }
        C1.finalize(data); C2.finalize(data); C3.finalize(data);
        ClusterCombination CC = new ClusterCombination(
                new ArrayList<>(Collections.singletonList(C1)),
                new ArrayList<>(Arrays.asList(C2,C3)),
                0
        );


//      Test get clusters (test if size is LHS+RHS)
        Assert.assertEquals(3, CC.getClusters().size());

//      Test bound (check if bounds are actually set and that LB <= UB)
        CC.bound(par.simMetric, true, par.getWl().get(0), par.getWr().get(1), par.pairwiseDistances);
        Assert.assertTrue(CC.getLB() >= -1);
        Assert.assertTrue(CC.getUB() >= -1);
        Assert.assertTrue(CC.getLB() <= CC.getUB());

        CC.checkAndSetLB(0);
        CC.checkAndSetUB(1);
        CC.checkAndSetLB(0.2);

        //      Test get and set bounds (set bounds, get bounds, check if you get proper min/max)
        Assert.assertEquals(0.2, CC.getLB(), 0.0001);

        //    Test get singletons (see if you get only ccs that are singletons)
        List<ClusterCombination> singletons = CC.getSingletons(par.getWl().get(0), par.getWr().get(1), false);
        for (ClusterCombination cc: singletons){
            Assert.assertEquals(3, cc.getClusters().size());
        }

//    Test get maxsubsetsimilarity (do with known CCs and similarities)
        Assert.assertEquals(-0.6783909485627608, CC.getMaxSubsetSimilarity(par), 0.001);
    }

    @Test
    public void testSingletonMethods(){
//        Make singleton cc
        ArrayList<Cluster> clusters = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Cluster C = new Cluster(par.getSimMetric().distFunc, i);
            C.finalize(data);
            C.setId(i);
            clusters.add(C);
        }
        ClusterCombination CC = new ClusterCombination(
                new ArrayList<>(Collections.singletonList(clusters.get(0))),
                new ArrayList<>(Arrays.asList(clusters.get(1), clusters.get(2))),
                0
        );

        // Test is singleton (make CC with one point)
        Assert.assertTrue(CC.isSingleton());

        //    Test to result tuple (check if everything is filled in properly, i.e. good size lists and right sim)
        CC.bound(par.simMetric, true, par.getWl().get(0), par.getWr().get(1), par.pairwiseDistances);
        ResultTuple resultTuple = CC.toResultTuple(par.headers);

        Assert.assertTrue(resultTuple.getLHS().get(0).equals(0));
        Assert.assertTrue(resultTuple.getRHS().get(0).equals(1));
        Assert.assertTrue(resultTuple.getRHS().get(1).equals(2));
        Assert.assertEquals(1, resultTuple.getLHS().size());
        Assert.assertEquals(2, resultTuple.getRHS().size());
        Assert.assertEquals(CC.getLB(), resultTuple.getSimilarity(), 0.001);
    }


    @Test

    public void testSplitting(){
//        Get Clusters C35, C33, C34
        HierarchicalClustering HC = new HierarchicalClustering(par);
        HC.run();
        Cluster[] allClusters = HC.getAllClusters();
        Cluster C1 = allClusters[35];
        Cluster C2 = allClusters[33];
        Cluster C3 = allClusters[34];

        List<Cluster> tmpRHS = new ArrayList<>(Arrays.asList(C2, C3, C2));

//    Test weightOverlap one side (both true and false)
        Assert.assertTrue(ClusterCombination.weightOverlapOneSide(C2, 0, tmpRHS, new double[]{0.4, 0.2, 0.4}));
        Assert.assertFalse(ClusterCombination.weightOverlapOneSide(C2, 0, tmpRHS, new double[]{0.4, 0.2, 0.3}));

//    Test weightOverlap two sides (both true and false)
        tmpRHS = new ArrayList<>(Arrays.asList(C2, C3));
        Assert.assertTrue(ClusterCombination.weightOverlapTwoSides(new ArrayList<>(Arrays.asList(C2, C3)), tmpRHS, new double[]{1,2}, new double[]{1,2}));
        Assert.assertFalse(ClusterCombination.weightOverlapTwoSides(new ArrayList<>(Arrays.asList(C1, C3)), tmpRHS, new double[]{1,1}, new double[]{1,2}));

//    Test cluster combination split (make CC with potential duplicates and see if they are filtered out)
        ClusterCombination CC = new ClusterCombination(
                new ArrayList<>(Arrays.asList(C1)),
                new ArrayList<>(Arrays.asList(C2.getParent(), C3)),
                0
        );

        List<ClusterCombination> splits = CC.split(new double[]{1}, new double[]{1,1}, false);
        Assert.assertEquals(1, splits.size());
        Assert.assertFalse(splits.get(0).RHS.contains(C1));
    }






}
