package similarities;

import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import org.junit.Assert;
import org.junit.Test;
import similarities.functions.Multipole;
import similarities.functions.PearsonCorrelation;
import tools.ClusterKit;

import java.util.ArrayList;
import java.util.Arrays;

public class MultipoleTest {
    private MultivariateSimilarityFunction simMetric = new Multipole();
    private ClusterKit kit = new ClusterKit(simMetric);

    @Test
    public void testSim(){
        double[] v1 = kit.data[kit.C1.get(0)];
        double[] v2 = kit.data[kit.C1.get(1)];

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

        double[] bounds = simMetric.empiricalDistanceBounds(kit.C1, kit.C2, kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

    @Test
    public void testEmpiricalSimilarityBounds1(){
        double lb = 0;
        double ub = 0;
        double maxLBSubset = 0;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1));
        ArrayList<Cluster> RHS = new ArrayList<>();
        ClusterBounds bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, kit.Wl.get(0), kit.Wr.get(1), kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }

    @Test
    public void testEmpiricalSimilarityBounds2(){
        double lb = -0.3226004270862144;
        double ub =0.8898022724944828;
        double maxLBSubset = 0;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1, kit.C2));
        ArrayList<Cluster> RHS = new ArrayList<>();
        ClusterBounds bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, kit.Wl.get(1), kit.Wr.get(1), kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }


    @Test
    public void testTheoreticalDistanceBounds(){
        double centroidDistance = lib.angle(kit.C1.getCentroid(), kit.C2.getCentroid());
        double lb = Math.max(0, centroidDistance - kit.C1.getRadius() - kit.C2.getRadius());
        double ub = Math.min(Math.PI, centroidDistance + kit.C1.getRadius() + kit.C2.getRadius());

        double[] bounds = simMetric.theoreticalDistanceBounds(kit.C1, kit.C2);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

    @Test
    public void testTheoreticalSimilarityBounds1(){
        double lb = 0;
        double ub = 0;
        double maxLBSubset = 0;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1));
        ArrayList<Cluster> RHS = new ArrayList<>();
        ClusterBounds bounds = simMetric.theoreticalSimilarityBounds(LHS, RHS, kit.Wl.get(0), kit.Wr.get(1));

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }

    @Test
    public void testTheoreticalSimilarityBounds2(){
        double lb = -1;
        double ub = 1;
        double maxLBSubset = 0;

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1, kit.C2));
        ArrayList<Cluster> RHS = new ArrayList<>();
        ClusterBounds bounds = simMetric.theoreticalSimilarityBounds(LHS, RHS, kit.Wl.get(1), kit.Wr.get(1));

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
        Assert.assertEquals(maxLBSubset, bounds.maxLowerBoundSubset, 0.0001);
    }

}
