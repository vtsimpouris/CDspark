package clustering;

import _aux.Pair;
import _aux.lib;
import core.Main;
import data_reading.DataReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import similarities.DistanceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


//    Test our kmeans++ clustering algorithm for multiple distance functions by testing if every point is always assigned to the closest cluster
public class ClusteringTest {
    private static Logger LOG = Main.getLogger(Level.INFO);
    private static String inputPath = "/home/jens/tue/data";
    private static int n = 50;
    private static int m = 100;
    private static double[][] data;
    private static double[][] pairwiseDistances;
    private static List<Integer> dataIds;



    @Before
    public void setUp(){
        Pair<String[], double[][]> dataPair = Main.getData("stock", inputPath, n, m, 0, LOG);
        double[][] rawData = dataPair.y;
        data = lib.l2norm(rawData);
        dataIds = IntStream.range(0,n).boxed().collect(Collectors.toList());
    }

    public void testClustering(List<Cluster> clusters, DistanceFunction distFunc){
        for (int i = 0; i < clusters.size(); i++) {
            Cluster cluster = clusters.get(i);
            for (int pid: cluster.tmpPointsIdx) {
                double distToCluster = cluster.distances.get(pid);
                for (int j = i+1; j < clusters.size(); j++) {
                    double distOtherCluster = distFunc.dist(data[pid], clusters.get(j).getCentroid());
                    Assert.assertTrue(distToCluster <= distOtherCluster);
                }
            }
        }
    }


    @Test
    public void testKmeansEuclidean() {
        DistanceFunction distFunc = lib::euclidean;
        pairwiseDistances = lib.computePairwiseDistances(data, distFunc, true);
        List<Cluster> clusters = Clustering.getKMeansMaxClusters(dataIds,data,pairwiseDistances, 5, 5, distFunc);
        testClustering(clusters, distFunc);
    }

    //Test angle distance
    @Test
    public void testKmeansAngle() {
        DistanceFunction distFunc = lib::angle;
        pairwiseDistances = lib.computePairwiseDistances(data, distFunc, true);
        List<Cluster> clusters = Clustering.getKMeansMaxClusters(dataIds,data,pairwiseDistances, 5, 5, distFunc);
        testClustering(clusters, distFunc);
    }


}
