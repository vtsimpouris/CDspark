package similarities;

import _aux.Pair;
import _aux.lib;
import algorithms.CorrelationDetective;
import bounding.ClusterBounds;
import clustering.Cluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import similarities.functions.PearsonCorrelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PearsonCorrelationTest {
    private double[][] data;
    private Cluster C1;
    private Cluster C2;
    private Cluster C3;
    private Cluster C4;
    private List<double[]> Wl = new ArrayList<>(Arrays.asList(new double[]{1}, new double[]{.5,.5}));
    private List<double[]> Wr = new ArrayList<>(Arrays.asList(new double[]{1}, new double[]{.5,.5}));
    private MultivariateSimilarityFunction simMetric = new PearsonCorrelation();
    private double[][] pairwiseDistances;

    private Pair<Cluster, double[][]> readCluster(String filename, int i){
//        Read vectors from file
        double[][] vectors = lib.l2norm(lib.readMatrix(filename));

//        Add points to cluster
        Cluster C = new Cluster(simMetric.distFunc, i++);
        for (int j = 1; j < vectors.length; j++) {
            C.addPoint(i++);
        }
        return new Pair<>(C, vectors);
    }

    private void readClusters(String rootdir) {
        int n = 0;

        String c1filename = rootdir + "midCluster0_dim100.csv";
        String c2filename = rootdir + "midCluster1_dim100.csv";
        String c3filename = rootdir + "midCluster2_dim100.csv";
        String c4filename = rootdir + "midCluster3_dim100.csv";

        Pair<Cluster, double[][]> out = readCluster(c1filename,n);
        C1 = out.x;
        C1.setId(1);
        double[][] c1data = out.y;
        n += c1data.length;

        out = readCluster(c2filename,n);
        C2 = out.x;
        C2.setId(2);
        double[][] c2data = out.y;
        n += c2data.length;

        out = readCluster(c3filename,n);
        C3 = out.x;
        C3.setId(3);
        double[][] c3data = out.y;
        n += c3data.length;

        out = readCluster(c4filename, n);
        C4 = out.x;
        C4.setId(4);
        double[][] c4data = out.y;
        n += c4data.length;

        double[][] data = new double[n][c1data[0].length];
        for (int i = 0; i < c1data.length; i++) {
            data[i] = c1data[i];
        }
        for (int i = 0; i < c2data.length; i++) {
            data[i + c1data.length] = c2data[i];
        }
        for (int i = 0; i < c3data.length; i++) {
            data[i + c1data.length + c2data.length] = c3data[i];
        }
        for (int i = 0; i < c4data.length; i++) {
            data[i + c1data.length + c2data.length + c3data.length] = c4data[i];
        }
        this.data = data;
        C1.finalize(data);
        C2.finalize(data);
        C3.finalize(data);
        C4.finalize(data);
        simMetric.setTotalClusters(4);
    }

    @Before public void setUp() {
        readClusters("output/clusters/dim100/");
        pairwiseDistances = lib.computePairwiseDistances(data, simMetric.distFunc,false);
    }

    @Test
    public void testSim(){
        double[] v1 = data[C1.get(0)];
        double[] v2 = data[C1.get(1)];

        double[] z1 = lib.l2norm(v1);
        double[] z2 = lib.l2norm(v2);

        double targetSim = lib.dot(z1, z2);
        double sim = simMetric.sim(z1,z2);

        Assert.assertEquals(targetSim, sim, 0.0001);
    }

    @Test
    public void testEmpiricalDistanceBounds(){
        double lb = 0.47388462439997503;
        double ub = 1.8992718439488459;

        double[] bounds = simMetric.empiricalDistanceBounds(C1, C2, pairwiseDistances);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

    @Test
    public void testEmpiricalSimilarityBounds3(){
        double lb = -.5334716909527332;
        double ub = 1;
        double maxLBSubset = -0.2732308830183487;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(C1));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(C3, C4));
        ClusterBounds bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, Wl.get(0), Wr.get(1), pairwiseDistances);

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }

    @Test
    public void testEmpiricalSimilarityBounds4(){
        double lb = -1;
        double ub = 1;
        double maxLBSubset = -0.2732308830183487;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(C1,C2));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(C3, C4));
        ClusterBounds bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, Wl.get(1), Wr.get(1), pairwiseDistances);

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }


    @Test
    public void testTheoreticalDistanceBounds(){
        double centroidDistance = lib.angle(C1.getCentroid(), C2.getCentroid());
        double lb = Math.max(0, centroidDistance - C1.getRadius() - C2.getRadius());
        double ub = Math.min(Math.PI, centroidDistance + C1.getRadius() + C2.getRadius());

        double[] bounds = simMetric.theoreticalDistanceBounds(C1, C2);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

}
