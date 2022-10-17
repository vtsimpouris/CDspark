package similarities.functions;

import _aux.Pair;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;

import java.util.List;

public class EuclideanSimilarity extends MultivariateSimilarityFunction {
    public EuclideanSimilarity() {
//        Use dotprod as distance function (has to do with empirical bounding)
        this.distFunc = lib::dot;

        this.MIN_SIMILARITY = 0;
        this.MAX_SIMILARITY = 1;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public double[][] preprocess(double[][] data) {
        return lib.l2norm(data);
    }

    @Override public double sim(double[] x, double[] y) {
        return 1 / (1 + Math.sqrt(2 - 2*this.distFunc.dist(x, y)));
    }

    @Override public double simToDist(double sim) {
        double d = 1 / sim - 1;

//        d2 to dot
        return 1 - ((d*d) / 2);
    }
    @Override public double distToSim(double dist) {
        return 1 / (1 + Math.sqrt(2 - 2*dist));
    }

    @Override public double[] theoreticalBounds(Cluster C1, Cluster C2){
        long ccID = getUniqueId(C1.id, C2.id);

        if (theoreticalPairwiseClusterCache.containsKey(ccID)) {
            return theoreticalPairwiseClusterCache.get(ccID);
        } else {
            double centroidDistance = this.distFunc.dist(C1.getCentroid(), C2.getCentroid());
            double lb = 1 / (1 + centroidDistance + C1.getRadius() + C2.getRadius());
            double ub = 1 / (1 + centroidDistance - C1.getRadius() - C2.getRadius());
            double[] bounds = new double[]{correctBound(lb), correctBound(ub)};
            theoreticalPairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    @Override public ClusterBounds theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr){
//        Get representation of aggregated clusters
        double[] CXc = aggCentroid(LHS, Wl);
        double CXr = aggRadius(LHS, Wl);

        double[] CYc = aggCentroid(RHS, Wr);
        double CYr = aggRadius(RHS, Wr);

        double lowerDist = lib.euclidean(CXc, CYc) - CXr - CYr;
        double upperDist = lib.euclidean(CXc, CYc) + CXr + CYr;

        double lower = 1 / (1 + upperDist);
        double upper = 1 / (1 + lowerDist);

//        Now get maxLowerBoundSubset
        double maxLowerBoundSubset = this.MIN_SIMILARITY;
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = theoreticalBounds(LHS.get(i), RHS.get(j));
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset);
    }

    @Override public ClusterBounds empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances){
        double betweenLower = 0;
        double betweenUpper = 0;
        double maxLowerBoundSubset = this.MIN_SIMILARITY;

//        Get all pairwise between cluster distances
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = empiricalBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                betweenLower += bounds[0] * Wl[i] * Wr[j];
                betweenUpper += bounds[1] * Wl[i] * Wr[j];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

//        Get all pairwise within cluster (side) distances LHS
        double withinLowerLHS = 0;
        double withinUpperLHS = 0;

//        Get all pairwise between cluster distances
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = i+1; j < LHS.size(); j++) {
                double[] bounds = empiricalBounds(LHS.get(i), LHS.get(j), pairwiseDistances);
                withinLowerLHS += bounds[0] * Wl[i] * Wl[j];
                withinUpperLHS += bounds[1] * Wl[i] * Wl[j];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

        //        Get all pairwise within cluster (side) distances RHS
        double withinLowerRHS = 0;
        double withinUpperRHS = 0;

//        Get all pairwise between cluster distances
        for (int i = 0; i < RHS.size(); i++) {
            for (int j = i+1; j < RHS.size(); j++) {
                double[] bounds = empiricalBounds(RHS.get(i), RHS.get(j), pairwiseDistances);
                withinLowerRHS += bounds[0] * Wr[i] * Wr[j];
                withinUpperRHS += bounds[1] * Wr[i] * Wr[j];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

        Pair<double[],double[]> weightSquares = getWeightSquaredSums(Wl, Wr);
        double WlSqSum = weightSquares.x[LHS.size() - 1];
        double WrSqSum = weightSquares.y[RHS.size() - 1];

//        Compute bounds
        double lowerD2 = WlSqSum + WrSqSum + 2*(withinLowerLHS + withinLowerRHS - betweenUpper);
        double upperD2 = WlSqSum + WrSqSum + 2*(withinUpperLHS + withinUpperRHS - betweenLower);

        double lower = 1 / (1 + Math.sqrt(upperD2));
        double upper = 1 / (1 + Math.sqrt(lowerD2));

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset);
    }



    private double[] aggCentroid(List<Cluster> clusters, double[] weights){
        double[] centroid = new double[clusters.get(0).getCentroid().length];
        for (int i = 0; i < clusters.size(); i++) {
            double w = weights[i];
            double[] wc = w == 1 ? clusters.get(i).getCentroid() : lib.scale(clusters.get(i).getCentroid(), w);
            centroid = lib.add(centroid, wc);
        }
        return centroid;
    }

    private double aggRadius(List<Cluster> clusters, double[] weights){
        double radius = 0;
        for (int i = 0; i < clusters.size(); i++) {
            radius += weights[i] * clusters.get(i).getRadius();
        }
        return radius;
    }
}
