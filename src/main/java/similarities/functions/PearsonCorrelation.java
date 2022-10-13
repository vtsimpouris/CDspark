package similarities.functions;

import _aux.lib;
import clustering.Cluster;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PearsonCorrelation extends MultivariateSimilarityFunction {
    private Double WlSqSum;
    private Double WrSqSum;
    public ConcurrentHashMap<Long, double[]> theoreticalPairwiseClusterCache = new ConcurrentHashMap<>();



    @Override public boolean hasEmpiricalBounds() {return true;}

    @Override public double[][] preprocess(double[][] data) {
        return lib.znorm(data);
    }
//    Angle distance
    public DistanceFunction distFunc = (double[] a, double[] b) -> Math.acos(sim(a, b));

//    Cosine similarity - normalized dot product
    @Override public double sim(double[] x, double[] y) {
        return lib.dot(x, y);
    }

    public double[] getBounds(List<Cluster> LHS, List<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr, boolean theoretical){
        double lower;
        double upper;

        double nominator_lower = 0;
        double nominator_upper = 0;

        double[] weightSquares = getWeightSquaredSums(Wl, Wr);

        //numerator: (nominator -- dyslexia strikes?!)
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = theoretical ? theoreticalBounds(LHS.get(i), RHS.get(j)): empiricalBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                nominator_lower += Wl[i] * Wr[j] * bounds[0];
                nominator_upper += Wl[i] * Wr[j] * bounds[1];
            }
        }

        //denominator: first sqrt
        double denominator_lower_left = weightSquares[0];
        double denominator_upper_left = weightSquares[0];

        for(int i=0; i< LHS.size(); i++){
            for(int j=i+1; j< LHS.size(); j++){
                double[] bounds = theoretical ? theoreticalBounds(LHS.get(i), RHS.get(j)): empiricalBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                denominator_lower_left += Wl[i] * Wl[j] * bounds[0];
                denominator_upper_left += Wl[i] * Wl[j] * bounds[1];

            }
        }

        //denominator: second sqrt
        double denominator_lower_right = weightSquares[1];
        double denominator_upper_right = weightSquares[1];

        for(int i=0; i< RHS.size(); i++){
            for(int j=i+1; j< RHS.size(); j++){
                double[] bounds = theoretical ? theoreticalBounds(LHS.get(i), RHS.get(j)): empiricalBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                denominator_lower_left += Wl[i] * Wl[j] * bounds[0];
                denominator_upper_left += Wl[i] * Wl[j] * bounds[1];

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

        return new double[]{lower, upper};
    }

    public double[] theoreticalBounds(Cluster C1, Cluster C2){
        long ccID = getUniqueId(C1.id, C2.id);

        if (theoreticalPairwiseClusterCache.containsKey(ccID)) {
            return theoreticalPairwiseClusterCache.get(ccID);
        } else {
            double centroidDistance = this.distFunc.dist(C1.centroid, C2.centroid);
            double lb = Math.cos(Math.min(Math.PI, centroidDistance + C1.radius + C2.radius));
            double ub = Math.cos(Math.max(0, centroidDistance - C1.radius - C2.radius));
            double[] bounds = new double[]{lb, ub};
            theoreticalPairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

//    Empirical bounds
    @Override public double[] empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr) {
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr, false);
    }

//    Theoretical bounds
    @Override public double[] theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr) {
        return getBounds(LHS, RHS, null, Wl, Wr, true);
    }


//    Get WlSqSum and WrSqSum if not already computed
    public double[] getWeightSquaredSums(double[] Wl, double[] Wr) {
        if (WlSqSum == null) {
            WlSqSum = 0d;
            for (double w : Wl) {
                WlSqSum += w * w;
            }
        }
        if (WrSqSum == null) {
            WrSqSum = 0d;
            for (double w : Wr) {
                WrSqSum += w * w;
            }
        }
        return new double[]{WlSqSum, WrSqSum};
    }

}
