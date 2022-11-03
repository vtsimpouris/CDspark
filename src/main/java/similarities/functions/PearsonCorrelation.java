package similarities.functions;

import _aux.Pair;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import org.apache.spark.api.java.function.Function2;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.util.List;
import java.util.function.Function;

public class PearsonCorrelation extends MultivariateSimilarityFunction  {
    private static final long serialVersionUID = -2685444218382696316L;
    public PearsonCorrelation() {
//        Angle is distance function
        //DistanceFunction lambda = (double[] a, double[] b) -> Math.acos(Math.min(Math.max(lib.dot(a, b), -1), 1));
        this.distFunc =  (double[] a, double[] b) -> Math.acos(Math.min(Math.max(lib.dot(a, b), -1),1));
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public double[] preprocess(double[] vector) {
        return lib.l2norm(vector);
    }
//    Angle distance

//    Cosine similarity - normalized dot product
    @Override public double sim(double[] x, double[] y) {
        return Math.min(Math.max(lib.dot(x, y),-1),1);
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

        Pair<Double, Double> weightSquares = getWeightSquaredSums(Wl, Wr);

        //numerator: (nominator -- dyslexia strikes?!)
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] angleBounds = empirical ? empiricalDistanceBounds(LHS.get(i), RHS.get(j), pairwiseDistances): theoreticalDistanceBounds(LHS.get(i), RHS.get(j));
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};
                nominator_lower += Wl[i] * Wr[j] * simBounds[0];
                nominator_upper += Wl[i] * Wr[j] * simBounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBounds[0]);
            }
        }

        //denominator: first sqrt
        double denominator_lower_left = weightSquares.x;
        double denominator_upper_left = weightSquares.x;

        for(int i=0; i< LHS.size(); i++){
            for(int j=i+1; j< LHS.size(); j++){
                double[] angleBounds = empirical ? empiricalDistanceBounds(LHS.get(i), LHS.get(j), pairwiseDistances): theoreticalDistanceBounds(LHS.get(i), LHS.get(j));
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};
                denominator_lower_left += 2 * Wl[i] * Wl[j] * simBounds[0];
                denominator_upper_left += 2 * Wl[i] * Wl[j] * simBounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBounds[0]);
            }
        }

        //denominator: second sqrt
        double denominator_lower_right = weightSquares.y;
        double denominator_upper_right = weightSquares.y;

        for(int i=0; i< RHS.size(); i++){
            for(int j=i+1; j< RHS.size(); j++){
                double[] angleBounds = empirical ? empiricalDistanceBounds(RHS.get(i), RHS.get(j), pairwiseDistances): theoreticalDistanceBounds(RHS.get(i), RHS.get(j));
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};
                denominator_lower_right += 2 * Wr[i] * Wr[j] * simBounds[0];
                denominator_upper_right += 2 * Wr[i] * Wr[j] * simBounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBounds[0]);
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

//    Empirical bounds
    @Override public ClusterBounds empiricalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr, true);
    }

//    Theoretical bounds
    @Override public ClusterBounds theoreticalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr) {
        return getBounds(LHS, RHS, null, Wl, Wr, false);
    }
}
