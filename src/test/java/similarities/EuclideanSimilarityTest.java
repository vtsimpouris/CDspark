package similarities;

import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import org.junit.Assert;
import org.junit.Test;
import similarities.functions.EuclideanSimilarity;
import similarities.functions.PearsonCorrelation;
import tools.ClusterKit;

import java.util.ArrayList;
import java.util.Arrays;

public class EuclideanSimilarityTest {
    private MultivariateSimilarityFunction simMetric = new EuclideanSimilarity();
    private ClusterKit kit = new ClusterKit(simMetric);

    @Test
    public void testSim(){
        double[] v1 = kit.data[kit.C1.get(0)];
        double[] v2 = kit.data[kit.C1.get(1)];

        double targetSim = 1 / (1 + lib.euclidean(v1,v2));
        double sim = simMetric.sim(v1,v2);

        Assert.assertEquals(targetSim, sim, 0.0001);
    }

    @Test
    public void testEmpiricalDistanceBounds(){
        double lb = 0.47388462439997503;
        double ub = 1.8992718439488459;

        double[] bounds = simMetric.empiricalDistanceBounds(kit.C1, kit.C2, kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

    @Test
    public void testEmpiricalSimilarityBounds3(){
        double lb = .3875616815589964;
        double ub = 1;
        double maxLBSubset = 0.6959959686945868;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(kit.C3, kit.C4));
        ClusterBounds bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, kit.Wl.get(0), kit.Wr.get(1), kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }

    @Test
    public void testEmpiricalSimilarityBounds4(){
        double lb = 0.38715426846176026;
        double ub = 1;
        double maxLBSubset = 0.6959959686945868;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1,kit.C2));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(kit.C3, kit.C4));
        ClusterBounds bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, kit.Wl.get(1), kit.Wr.get(1), kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }


    @Test
    public void testTheoreticalDistanceBounds(){
        double centroidDistance = lib.euclidean(kit.C1.getCentroid(), kit.C2.getCentroid());
        double lb = Math.max(0,centroidDistance - kit.C1.getRadius() - kit.C2.getRadius());
        double ub = Math.max(0,centroidDistance + kit.C1.getRadius() + kit.C2.getRadius());

        double[] bounds = simMetric.theoreticalDistanceBounds(kit.C1, kit.C2);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

    @Test
    public void testTheoreticalSimilarityBounds3(){
        double[] CXc = kit.C1.getCentroid();
        double CXr = kit.C1.getRadius();
        double[] CYc = lib.scale(lib.add(kit.C3.getCentroid(), kit.C4.getCentroid()), .5);
        double CYr = (kit.C3.getRadius() + kit.C4.getRadius()) / 2;

        double centroidDistance = lib.euclidean(CXc, CYc);
        double lb = 1 / (1 + Math.max(0,centroidDistance + CXr + CYr));
        double ub = 1 / (1 + Math.max(0,centroidDistance - CXr - CYr));

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(kit.C3, kit.C4));
        ClusterBounds bounds = simMetric.theoreticalSimilarityBounds(LHS, RHS, kit.Wl.get(0), kit.Wr.get(1));

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
    }

    @Test
    public void testTheoreticalSimilarityBounds4(){
        double[] CXc = lib.scale(lib.add(kit.C1.getCentroid(), kit.C2.getCentroid()), .5);
        double CXr = (kit.C1.getRadius() + kit.C2.getRadius()) / 2;
        double[] CYc = lib.scale(lib.add(kit.C3.getCentroid(), kit.C4.getCentroid()), .5);
        double CYr = (kit.C3.getRadius() + kit.C4.getRadius()) / 2;

        double centroidDistance = lib.euclidean(CXc, CYc);
        double lb = 1 / (1 + Math.max(0,centroidDistance + CXr + CYr));
        double ub = 1 / (1 + Math.max(0,centroidDistance - CXr - CYr));

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1, kit.C2));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(kit.C3, kit.C4));
        ClusterBounds bounds = simMetric.theoreticalSimilarityBounds(LHS, RHS, kit.Wl.get(1), kit.Wr.get(1));

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
    }

}
