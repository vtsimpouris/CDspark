package similarities;

import _aux.Pair;
import bounding.ClusterBounds;
import bounding.ClusterCombination;
import clustering.Cluster;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public abstract class MultivariateSimilarityFunction {
    @Setter int totalClusters;
    public DistanceFunction distFunc;
    public double MAX_SIMILARITY = 1;
    public double MIN_SIMILARITY = -1;
    public AtomicLong nLookups = new AtomicLong(0);

    public ConcurrentHashMap<Long, double[]> empiricalPairwiseClusterCache = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Long, double[]> theoreticalPairwiseClusterCache = new ConcurrentHashMap<>();

    //    WlSqSum for each subset of Wl (i.e. [0], [0,1], [0,1,2], ...)
    private double[] WrSqSum;
    private double[] WlSqSum;

//    ----------------------- METHODS --------------------------------
    public String toString(){
        return this.getClass().getSimpleName();
    }

    public abstract boolean hasEmpiricalBounds();
    public abstract boolean isTwoSided();
    public abstract double[][] preprocess(double[][] data);

    public abstract double sim(double[] x, double[] y);
    public abstract double simToDist(double sim);
    public abstract double distToSim(double dist);
    public abstract ClusterBounds empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances);
    public abstract ClusterBounds theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr);
    public abstract double[] theoreticalBounds(Cluster C1, Cluster C2);

    public double[] empiricalBounds(Cluster C1, Cluster C2, double[][] pairwiseDistances){
        long ccID = getUniqueId(C1.id, C2.id);

        if (empiricalPairwiseClusterCache.containsKey(ccID)) {
            return empiricalPairwiseClusterCache.get(ccID);
        } else {
            double lb = Double.MAX_VALUE;
            double ub = -Double.MAX_VALUE;
            for (int i = 0; i < C1.size(); i++) {
                for (int j = 0; j < C2.size(); j++) {
                    double sim = distToSim(pairwiseDistances[C1.get(i)][C2.get(j)]);
                    lb = Math.min(lb, sim);
                    ub = Math.max(ub, sim);
                    nLookups.incrementAndGet();
                }
            }
            double[] bounds = new double[]{correctBound(lb), correctBound(ub)};
            empiricalPairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    public long getUniqueId(int id1, int id2) {
        if (id1 < id2) {
            return (long) id1 * this.totalClusters + id2;
        } else {
            return (long) id2 * this.totalClusters + id1;
        }
    }

    public double correctBound(double bound){
        return Math.min(Math.max(bound, MIN_SIMILARITY), MAX_SIMILARITY);
    }


//    TODO COULD PUSH DOWN TO ADDITIONAL ABSTRACTION
    public Pair<double[], double[]> getWeightSquaredSums(double[] Wl, double[] Wr) {
        if (WlSqSum == null) {
            WlSqSum = new double[Wl.length];
            double runSumSq = 0;
            for (int i = 0; i < Wl.length; i++) {
                runSumSq += Wl[i] * Wl[i];
                WlSqSum[i] = runSumSq;
            }
        }
        if (WrSqSum == null) {
            WrSqSum = new double[Wr.length];
            double runSumSq = 0;
            for (int i = 0; i < Wr.length; i++) {
                runSumSq += Wr[i] * Wr[i];
                WrSqSum[i] = runSumSq;
            }
        }

        return new Pair<>(WlSqSum, WrSqSum);
    }
}

