package similarities.functions;

import _aux.Pair;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;

import java.util.List;

public class PearsonCorrelation extends MultivariateSimilarityFunction {
    public PearsonCorrelation() {
        this.distFunc = (double[] a, double[] b) -> Math.acos(Math.min(Math.max(lib.dot(a, b) / a.length, -1),1));
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public double[][] preprocess(double[][] data) {
        return lib.znorm(data);
    }
//    Angle distance

//    Cosine similarity - normalized dot product
    @Override public double sim(double[] x, double[] y) {
        return Math.min(Math.max(lib.dot(x, y) / x.length, -1),1);
    }

    @Override public double simToDist(double sim) {
        return Math.acos(sim);
    }
    @Override public double distToSim(double dist) {return Math.cos(dist);}

    public ClusterBounds getBounds(List<Cluster> LHS, List<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr, boolean empirical){
        double lower;
        double upper;
        double maxLowerBoundSubset = -1;

        double nominator_lower = 0;
        double nominator_upper = 0;

        Pair<double[],double[]> weightSquares = getWeightSquaredSums(Wl, Wr);

        //numerator: (nominator -- dyslexia strikes?!)
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = empirical ? empiricalBounds(LHS.get(i), RHS.get(j), pairwiseDistances): theoreticalBounds(LHS.get(i), RHS.get(j));
//                TODO HOW DO YOU NORMALIZE THESE WEIGHTS IF WE ARE COMPARING SUBSET COMBINATIONS?
                nominator_lower += Wl[i] * Wr[j] * bounds[0];
                nominator_upper += Wl[i] * Wr[j] * bounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

        //denominator: first sqrt
        double denominator_lower_left = weightSquares.x[LHS.size() - 1];
        double denominator_upper_left = weightSquares.x[LHS.size() - 1];

        for(int i=0; i< LHS.size(); i++){
            for(int j=i+1; j< LHS.size(); j++){
                double[] bounds = empirical ? empiricalBounds(LHS.get(i), LHS.get(j), pairwiseDistances): theoreticalBounds(LHS.get(i), LHS.get(j));
                denominator_lower_left += Wl[i] * Wl[j] * 2*bounds[0];
                denominator_upper_left += Wl[i] * Wl[j] * 2*bounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

        //denominator: second sqrt
        double denominator_lower_right = weightSquares.y[RHS.size() - 1];
        double denominator_upper_right = weightSquares.y[RHS.size() - 1];

        for(int i=0; i< RHS.size(); i++){
            for(int j=i+1; j< RHS.size(); j++){
                double[] bounds = empirical ? empiricalBounds(RHS.get(i), RHS.get(j), pairwiseDistances): theoreticalBounds(RHS.get(i), RHS.get(j));
                denominator_lower_right += Wr[i] * Wr[j] * 2*bounds[0];
                denominator_upper_right += Wr[i] * Wr[j] * 2*bounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, bounds[0]);
            }
        }

        //denominator: whole. note that if bounds are too loose we could get a non-positive value, while this is not possible due to Pos. Def. of variance.
        double denominator_lower = Math.sqrt(Math.max(denominator_lower_left, 1e-7)*Math.max(denominator_lower_right, 1e-7));
        double denominator_upper = Math.sqrt(Math.max(denominator_upper_left, 1e-7)*Math.max(denominator_upper_right, 1e-7));

        //case distinction for final bound
        if (nominator_lower >= 0) {
            lower = nominator_lower / denominator_upper;
            upper = nominator_upper / denominator_lower;
        } else if (nominator_lower < 0 && nominator_upper >= 0) {
            lower = nominator_lower / denominator_lower;
            upper = nominator_upper / denominator_lower;
        } else if (nominator_upper < 0) {
            lower = nominator_lower / denominator_lower;
            upper = nominator_upper / denominator_upper;
        } else {
            lower = -1000;
            upper = 1000;
        }

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset);
    }

    @Override public double[] theoreticalBounds(Cluster C1, Cluster C2){
        long ccID = getUniqueId(C1.id, C2.id);

        if (theoreticalPairwiseClusterCache.containsKey(ccID)) {
            return theoreticalPairwiseClusterCache.get(ccID);
        } else {
            double centroidDistance = this.distFunc.dist(C1.getCentroid(), C2.getCentroid());
            double lb = Math.cos(Math.min(Math.PI, centroidDistance + C1.getRadius() + C2.getRadius()));
            double ub = Math.cos(Math.max(0, centroidDistance - C1.getRadius() - C2.getRadius()));
            double[] bounds = new double[]{lb, ub};
            theoreticalPairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

//    Empirical bounds
    @Override public ClusterBounds empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr, true);
    }

//    Theoretical bounds
    @Override public ClusterBounds theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr) {
        return getBounds(LHS, RHS, null, Wl, Wr, false);
    }
}
