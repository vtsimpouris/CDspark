package similarities;

import bounding.ClusterBounds;
import bounding.ClusterCombination;
import clustering.Cluster;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class MultivariateSimilarityFunction {
    @Setter int totalClusters;
    public DistanceFunction distFunc;
    public double MAX_SIMILARITY = 1;
    public double MIN_SIMILARITY = -1;

    public ConcurrentHashMap<Long, double[]> empiricalPairwiseClusterCache = new ConcurrentHashMap<>();

//    ----------------------- METHODS --------------------------------

    public abstract boolean hasEmpiricalBounds();
    public abstract double[][] preprocess(double[][] data);

    public abstract double sim(double[] x, double[] y);
    public abstract double simToDist(double sim);
    public abstract double distToSim(double dist);
    public abstract ClusterBounds empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr);
    public abstract ClusterBounds theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr);

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

}

