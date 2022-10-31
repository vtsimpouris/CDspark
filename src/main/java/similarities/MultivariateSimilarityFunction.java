package similarities;

import _aux.Pair;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class MultivariateSimilarityFunction {
    @Setter int totalClusters;
    @Getter public DistanceFunction distFunc;
    public double MAX_SIMILARITY = 1;
    public double MIN_SIMILARITY = -1;
    public AtomicLong nLookups = new AtomicLong(0);

    public ConcurrentHashMap<Long, double[]> pairwiseClusterCache = new ConcurrentHashMap<>(100000, .4f);

    //    WlSqSum for each subset of Wl (i.e. [0], [0,1], [0,1,2], ...)
    private Map<Integer, Double> WlSqSum = new HashMap<>(4);
    private Map<Integer, Double> WrSqSum = new HashMap<>(4);

//    ----------------------- METHODS --------------------------------
    public String toString(){
        return this.getClass().getSimpleName();
    }

    public abstract boolean hasEmpiricalBounds();
    public abstract boolean isTwoSided();
    public abstract double sim(double[] x, double[] y);

    public double[][] preprocess(double[][] data){
        for (int i = 0; i < data.length; i++) {
            data[i] = preprocess(data[i]);
        }
        return data;
    };
    public abstract double[] preprocess(double[] vector);

    public abstract double simToDist(double sim);
    public abstract double distToSim(double dist);
    public abstract ClusterBounds theoreticalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr);
    public double[] theoreticalDistanceBounds(Cluster C1, Cluster C2){
        long ccID = hashPairwiseCluster(C1.id, C2.id);

        if (pairwiseClusterCache.containsKey(ccID)) {
            return pairwiseClusterCache.get(ccID);
        } else {
            double centroidDistance = this.distFunc.dist(C1.getCentroid(), C2.getCentroid());
            double lbDist = Math.max(0,centroidDistance - C1.getRadius() - C2.getRadius());
            double ubDist = Math.max(0,centroidDistance + C1.getRadius() + C2.getRadius());
            double[] bounds = new double[]{lbDist, ubDist};
            pairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    public abstract ClusterBounds empiricalSimilarityBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances);

    public double[] empiricalDistanceBounds(Cluster C1, Cluster C2, double[][] pairwiseDistances){
        long ccID = hashPairwiseCluster(C1.id, C2.id);

        if (pairwiseClusterCache.containsKey(ccID)) {
            return pairwiseClusterCache.get(ccID);
        } else {
            double lb = Double.MAX_VALUE;
            double ub = -Double.MAX_VALUE;
            for (int i = 0; i < C1.size(); i++) {
                for (int j = 0; j < C2.size(); j++) {
                    double dist = pairwiseDistances[C1.get(i)][C2.get(j)];
                    lb = Math.min(lb, dist);
                    ub = Math.max(ub, dist);
                    nLookups.incrementAndGet();
                }
            }
            double[] bounds = new double[]{lb, ub};
            pairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    public long hashPairwiseCluster(int id1, int id2) {
        if (id1 < id2) {
            return (long) id1 * this.totalClusters + id2;
        } else {
            return (long) id2 * this.totalClusters + id1;
        }
    }

    public Pair<List<Integer>, Integer> hashMultiCluster(List<Cluster> LHS, List<Cluster> RHS) {
        ArrayList<Integer> ids = new ArrayList<>(LHS.size() + RHS.size() + 1);
        for (Cluster c : LHS) {
            ids.add(c.id);
        }
        ids.add(-1); // add for side separation
        for (Cluster c : RHS) {
            ids.add(c.id);
        }
        return new Pair<>(ids, ids.hashCode());
    }

    public double correctBound(double bound){
        return Math.min(Math.max(bound, MIN_SIMILARITY), MAX_SIMILARITY);
    }


//    TODO COULD PUSH DOWN TO ADDITIONAL ABSTRACTION
    public Pair<Double,Double> getWeightSquaredSums(double[] Wl, double[] Wr) {
        if (!WlSqSum.containsKey(Wl.length)) {
            double runSumSq = 0;
            for (int i = 0; i < Wl.length; i++) {
                runSumSq += Wl[i] * Wl[i];
            }
            WlSqSum.put(Wl.length, runSumSq);
        }

        if (!WrSqSum.containsKey(Wr.length)) {
            double runSumSq = 0;
            for (int i = 0; i < Wr.length; i++) {
                runSumSq += Wr[i] * Wr[i];
            }
            WrSqSum.put(Wr.length, runSumSq);
        }

        return new Pair<>(WlSqSum.get(Wl.length), WrSqSum.get(Wr.length));
    }

}

