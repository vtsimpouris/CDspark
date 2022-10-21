package similarities;

import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import org.junit.Assert;
import org.junit.Test;
import similarities.functions.ChebyshevSimilarity;
import similarities.functions.MinkowskiSimilarity;
import tools.ClusterKit;

import java.util.ArrayList;
import java.util.Arrays;

public class ChebyshevSimilarityTest {
    private MultivariateSimilarityFunction simMetric = new ChebyshevSimilarity();
    private ClusterKit kit = new ClusterKit(simMetric);

    @Test
    public void testSim(){
        double[] v1 = kit.data[kit.C1.get(0)];
        double[] v2 = kit.data[kit.C1.get(1)];

        double targetSim = 1 / (1 + lib.chebyshev(v1,v2));
        double sim = simMetric.sim(v1,v2);

        Assert.assertEquals(targetSim, sim, 0.0001);
    }

    @Test
    public void testEmpiricalDistanceBounds(){
        double lb = 0.09335154476410563;
        double ub = 0.596542269476989;

        double[] bounds = simMetric.empiricalDistanceBounds(kit.C1, kit.C2, kit.pairwiseDistances);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, bounds[1], 0.0001);
    }

    @Test
    public void testTheoreticalDistanceBounds(){
        double centroidDistance = lib.chebyshev(kit.C1.getCentroid(), kit.C2.getCentroid());
        double lb = Math.max(0,centroidDistance - kit.C1.getRadius() - kit.C2.getRadius());
        double ub = Math.max(0,centroidDistance + kit.C1.getRadius() + kit.C2.getRadius());

        double[] bounds = simMetric.theoreticalDistanceBounds(kit.C1, kit.C2);

        Assert.assertEquals(lb, bounds[0], 0.0001);
        Assert.assertEquals(ub, Math.min(Math.PI,bounds[1]), 0.0001);
    }

    @Test
    public void testTheoreticalSimilarityBounds3(){
        double[] CXc = kit.C1.getCentroid();
        double CXr = kit.C1.getRadius();
        double[] CYc = lib.scale(lib.add(kit.C3.getCentroid(), kit.C4.getCentroid()), .5);
        double CYr = (kit.C3.getRadius() + kit.C4.getRadius()) / 2;

        double centroidDistance = lib.chebyshev(CXc, CYc);
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

        double centroidDistance = lib.chebyshev(CXc, CYc);
        double lb = 1 / (1 + Math.max(0,centroidDistance + CXr + CYr));
        double ub = 1 / (1 + Math.max(0,centroidDistance - CXr - CYr));

        ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(kit.C1, kit.C2));
        ArrayList<Cluster> RHS = new ArrayList<>(Arrays.asList(kit.C3, kit.C4));
        ClusterBounds bounds = simMetric.theoreticalSimilarityBounds(LHS, RHS, kit.Wl.get(1), kit.Wr.get(1));

        Assert.assertEquals(lb, bounds.LB, 0.0001);
        Assert.assertEquals(ub, bounds.UB, 0.0001);
    }

}
