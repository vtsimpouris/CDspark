package similarities;

import clustering.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class MultivariateSimilarityFunction {
    public abstract double[][] preprocess(double[][] data);

    public DistanceFunction dist;
    public abstract DistanceFunction getDistFunction();
    public abstract double sim(double[] x, double[] y);
    public abstract double[] empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[][] pairwiseDistances, double[] weights);
    public abstract double[] theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] weights);

//    Cache of empirical bounds
    public ArrayList<ConcurrentHashMap<Long, double[]>> empiricalBoundsCache = new ArrayList<>();

//    Initialize cache
    public void initEmpiricalBoundsCache(int n) {
        for (int i = 0; i < n; i++) {
            empiricalBoundsCache.add(new ConcurrentHashMap<>((int) (2153509/0.4), 0.4f));
        }
    }

}

