package similarities.functions;

import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;

import java.util.List;
public class MinkowskiSimilarity extends MultivariateSimilarityFunction {

    public MinkowskiSimilarity(int p) {
        this.distFunc = (double[] a, double[] b) -> lib.minkowski(a, b, p);
        this.MIN_SIMILARITY = 0;
        this.MAX_SIMILARITY = 1;
    }

    @Override public boolean hasEmpiricalBounds() {return false;}
    @Override public boolean isTwoSided() {return true;}
    @Override public double[][] preprocess(double[][] data) {
        return lib.l2norm(data);
    }

    @Override public double sim(double[] x, double[] y) {
        return 1 / (1 + this.distFunc.dist(x, y));
    }

    @Override public double simToDist(double sim) {
        return 1 / sim - 1;
    }

    @Override public double distToSim(double dist) {
        return 1 / (1 + dist);
    }

    @Override public double[] theoreticalDistanceBounds(Cluster C1, Cluster C2){
        long ccID = getUniqueId(C1.id, C2.id);

        if (theoreticalPairwiseClusterCache.containsKey(ccID)) {
            return theoreticalPairwiseClusterCache.get(ccID);
        } else {
            double centroidDistance = this.distFunc.dist(C1.getCentroid(), C2.getCentroid());

            double lowerDist = centroidDistance - C1.getRadius() - C2.getRadius();
            double upperDist = centroidDistance + C1.getRadius() + C2.getRadius();
            double[] bounds = new double[]{lowerDist, upperDist};
            theoreticalPairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    //    Compute theoretical similarity bounds for a set of clusters
    @Override public ClusterBounds theoreticalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr) {
        //        Get representation of aggregated clusters
        double[] CXc = aggCentroid(LHS, Wl);
        double CXr = aggRadius(LHS, Wl);

        double[] CYc = aggCentroid(RHS, Wr);
        double CYr = aggRadius(RHS, Wr);

        double centroidDistance = this.distFunc.dist(CXc, CYc);

        double lowerDist = Math.max(0,centroidDistance - CXr - CYr);
        double upperDist = Math.max(0,centroidDistance + CXr + CYr);

        double lowerSim = 1 / (1 + upperDist);
        double upperSim = 1 / (1 + lowerDist);

//        Now get maxLowerBoundSubset
        double maxLowerBoundSubset = this.MIN_SIMILARITY;
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = theoreticalDistanceBounds(LHS.get(i), RHS.get(j));
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, 1 / (1 + bounds[1]));
            }
        }

        return new ClusterBounds(correctBound(lowerSim), correctBound(upperSim), maxLowerBoundSubset);
    }

    @Override public ClusterBounds empiricalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
        throw new RuntimeException("Empirical bounds not implemented for this similarity function");
    }

    public static double[] aggCentroid(List<Cluster> clusters, double[] weights){
        double[] centroid = new double[clusters.get(0).getCentroid().length];
        for (int i = 0; i < clusters.size(); i++) {
            double w = weights[i];
            double[] wc = w == 1 ? clusters.get(i).getCentroid() : lib.scale(clusters.get(i).getCentroid(), w);
            centroid = lib.add(centroid, wc);
        }
        return centroid;
    }

    public static double aggRadius(List<Cluster> clusters, double[] weights){
        double radius = 0;
        for (int i = 0; i < clusters.size(); i++) {
            radius += weights[i] * clusters.get(i).getRadius();
        }
        return radius;
    }
}
