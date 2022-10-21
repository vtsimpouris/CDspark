package similarities.functions;

import _aux.Pair;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;

import java.util.List;

import static similarities.functions.MinkowskiSimilarity.aggCentroid;
import static similarities.functions.MinkowskiSimilarity.aggRadius;

public class EuclideanSimilarity extends MultivariateSimilarityFunction {
    public EuclideanSimilarity() {
//        Angle is distance function
        this.distFunc = (double[] a, double[] b) -> Math.acos(Math.min(Math.max(lib.dot(a, b), -1),1));

        this.MIN_SIMILARITY = 0;
        this.MAX_SIMILARITY = 1;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public double[] preprocess(double[] vector) {
        return lib.l2norm(vector);
    }

    @Override public double sim(double[] x, double[] y) {
        return 1 / (1 + Math.sqrt(2 - 2*Math.cos(this.distFunc.dist(x, y))));
    }

    @Override public double simToDist(double sim) {
        double d = 1 / sim - 1;

//        d2 to dot
        return Math.acos(1 - ((d*d) / 2));
    }

    @Override public double distToSim(double dist) {
        return 1 / (1 + Math.sqrt(2 - 2*Math.cos(dist)));
    }

    @Override public ClusterBounds theoreticalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr){
//        Get representation of aggregated clusters
        double[] CXc = aggCentroid(LHS, Wl);
        double CXr = aggRadius(LHS, Wl);

        double[] CYc = aggCentroid(RHS, Wr);
        double CYr = aggRadius(RHS, Wr);

        double centroidDistance = lib.euclidean(CXc, CYc);

        double lowerDist = Math.max(0,centroidDistance - CXr - CYr);
        double upperDist = Math.max(0,centroidDistance + CXr + CYr);

        double lowerSim = 1 / (1 + upperDist);
        double upperSim = 1 / (1 + lowerDist);

//        Now get maxLowerBoundSubset
        double maxLowerBoundSubset = this.MIN_SIMILARITY;
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] angleBounds = theoreticalDistanceBounds(LHS.get(i), RHS.get(j));
                double simBound = this.distToSim(Math.min(Math.PI, angleBounds[1]));

                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBound);
            }
        }

        return new ClusterBounds(correctBound(lowerSim), correctBound(upperSim), maxLowerBoundSubset);
    }

    @Override public ClusterBounds empiricalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances){
        double betweenLowerDot = 0;
        double betweenUpperDot = 0;

        double withinLowerDot = 0;
        double withinUpperDot = 0;

        double maxLowerBoundSubset = this.MIN_SIMILARITY;

//        Get all pairwise between cluster distances
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = empiricalDistanceBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                betweenLowerDot -= 2 * Wl[i] * Wr[j] * Math.cos(bounds[0]);
                betweenUpperDot -= 2 * Wl[i] * Wr[j] * Math.cos(bounds[1]);
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(bounds[0]));
            }
        }


//        Get all pairwise within cluster (side) distances LHS
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = i+1; j < LHS.size(); j++) {
                double[] bounds = empiricalDistanceBounds(LHS.get(i), LHS.get(j), pairwiseDistances);
                withinLowerDot += 2 * Wl[i] * Wl[j] * Math.cos(bounds[0]);
                withinUpperDot += 2 * Wl[i] * Wl[j] * Math.cos(bounds[1]);
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(bounds[0]));
            }
        }

        //        Get all pairwise within cluster (side) distances RHS
        for (int i = 0; i < RHS.size(); i++) {
            for (int j = i+1; j < RHS.size(); j++) {
                double[] bounds = empiricalDistanceBounds(RHS.get(i), RHS.get(j), pairwiseDistances);
                withinLowerDot += 2 * Wr[i] * Wr[j] * Math.cos(bounds[0]);
                withinUpperDot += 2 * Wr[i] * Wr[j] * Math.cos(bounds[1]);
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(bounds[0]));
            }
        }

        Pair<Double, Double> weightSquares = getWeightSquaredSums(Wl, Wr);
        double wSqSum = weightSquares.x + weightSquares.y;

//        Compute bounds
        double lowerD = Math.sqrt(Math.max(0,wSqSum + Math.min(betweenLowerDot, betweenUpperDot) + Math.min(withinLowerDot, withinUpperDot)));
        double upperD = Math.sqrt(Math.max(0,wSqSum + Math.max(betweenLowerDot, betweenUpperDot) + Math.max(withinLowerDot, withinUpperDot)));

        double lower = 1 / (1 + upperD);
        double upper = 1 / (1 + lowerD);

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset);
    }




}
