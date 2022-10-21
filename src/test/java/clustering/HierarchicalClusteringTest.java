package clustering;

import _aux.Pair;
import _aux.lib;
import algorithms.AlgorithmEnum;
import core.Main;
import core.Parameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.EuclideanSimilarity;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;


//    Test our kmeans++ clustering algorithm for multiple distance functions by testing if every point is always assigned to the closest cluster
public class HierarchicalClusteringTest {
    private final static Logger LOG = Main.getLogger(Level.INFO);
    private final static String inputPath = "/home/jens/tue/data";
    private final static int n = 50;
    private final static int m = 100;
    private static double[][] data;
    private static double[][] pairwiseDistances;

    private final static MultivariateSimilarityFunction simMetric = new EuclideanSimilarity();
    private final static double startEpsilon = 1 / .6 - 1;
    private final static int defaultDesiredClusters = 5;
    private final static double epsilonMultiplier = 0.8;
    private final static int maxLevels = 20;
    private final static ClusteringAlgorithmEnum clusteringAlgorithm = ClusteringAlgorithmEnum.KMEANS;
    private final static int breakFirstKLevelsToMoreClusters = 1;
    private final static int clusteringRetries = 5;

    private HierarchicalClustering HC;
    private Parameters par;

    private void getParameters(){
        par = new Parameters(
                LOG, "", "", false, false, "", 1, AlgorithmEnum.SIMILARITY_DETECTIVE,
                false, false, 1, simMetric, "", new ArrayList<>(), new ArrayList<>(),
                1, 1, false, "stock", "", new String[0], data, n, m,
                1, false, 0.5, 0.5, startEpsilon, epsilonMultiplier, maxLevels, defaultDesiredClusters,
                clusteringAlgorithm, breakFirstKLevelsToMoreClusters, clusteringRetries, 0, 0, 0, -1, ""
        );
        par.init();
    }


    @Before
    public void setUp(){
        Pair<String[], double[][]> dataPair = Main.getData("stock", inputPath, n, m, 0, LOG);
        double[][] rawData = dataPair.y;
        data = lib.l2norm(rawData);

        getParameters();
        par.setPairwiseDistances(lib.computePairwiseDistances(data, simMetric.distFunc, true));

//        Make hierarchical clustering
        HC = new HierarchicalClustering(par);
        HC.run();
    }

//    Run all tests (with setup once)
    @Test
    public void testHierarchicalClustering(){
        testRootCluster();
        testFirstLevelsClusterBreak();
        testNumberOfClustersPerLevel();
        testMaxClusterLevels();
        testLeaves();
    }

    //    Test root cluster size
    public void testRootCluster(){
        Assert.assertNotNull(HC.clusterTree.get(0));
        Assert.assertNotNull(HC.clusterTree.get(0).get(0));
        Cluster root = HC.clusterTree.get(0).get(0);
        Assert.assertTrue(root.finalized);
        Assert.assertEquals(n, root.pointsIdx.size());
    }

//    Test first k levels cluster breaks
    public void testFirstLevelsClusterBreak(){
        for(int i=0; i<breakFirstKLevelsToMoreClusters; i++){
            List<Cluster> currentKCLevel = HC.clusterTree.get(i);
            for(Cluster cluster : currentKCLevel){
                Assert.assertTrue(cluster.children.size() > breakFirstKLevelsToMoreClusters);
            }
        }
    }

//    Test number of clusters per level
    public void testNumberOfClustersPerLevel(){
        for(int i=breakFirstKLevelsToMoreClusters; i<HC.clusterTree.size(); i++){
            List<Cluster> currentKCLevel = HC.clusterTree.get(i);
            for(Cluster cluster : currentKCLevel){
                Assert.assertTrue(cluster.finalized);
                Assert.assertTrue(cluster.children.size() <= defaultDesiredClusters);
            }
        }
    }

//    Test max cluster levels
    public void testMaxClusterLevels(){
        Assert.assertTrue(HC.clusterTree.size() <= maxLevels);
    }

//    Test all leaves are singletons
    public void testLeaves(){
        List<Cluster> lastLevel = HC.clusterTree.get(HC.clusterTree.size()-1);
        for (Cluster cluster: lastLevel){
            Assert.assertTrue(cluster.finalized);
            Assert.assertEquals(1, cluster.pointsIdx.size());
        }
    }
}
