package algorithms.baselines;

import _aux.Parameters;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SmartBaseline extends Baseline{
    Cluster[] singletonClusters;

    public SmartBaseline(Parameters par) {
        super(par);
    }

    @Override
    public void prepare(){
        par.setPairwiseDistances(lib.computePairwiseDistances(par.data, par.simMetric.distFunc, par.parallel));
        makeSingletonClusters();
    }

    private void makeSingletonClusters(){
        singletonClusters = new Cluster[par.n];
        for(int i = 0; i < par.n; i++){
            Cluster c = new Cluster(par.simMetric.distFunc, i);
            c.finalize(par.data);
            singletonClusters[i] = c;
        }
        par.simMetric.setTotalClusters(par.n);
    }

//    Compute similarities based on pairwise similarities (if possible)
    @Override
    public double computeSimilarity(List<Integer> left, List<Integer> right){
        long hash = hashCandidate(left, right);
        if(similarityCache.containsKey(hash)){
            return similarityCache.get(hash);
        }
        else{
            double sim;

//            If simmetric is distributive, get similarity based on pairwise similarities
            if (par.simMetric.hasEmpiricalBounds()){
                List<Cluster> LHS = left.stream().map(i -> singletonClusters[i]).collect(Collectors.toList());
                List<Cluster> RHS = right.stream().map(i -> singletonClusters[i]).collect(Collectors.toList());
                ClusterBounds bounds = par.simMetric.empiricalSimilarityBounds(LHS, RHS,
                        par.Wl.get(left.size() - 1), par.Wr.get(right.size() - 1), par.pairwiseDistances);
                sim = bounds.LB;
            } else {
//            Make linear combinations
            double[] v1 = par.simMetric.preprocess(linearCombination(left, WlFull));
            double[] v2 = par.simMetric.preprocess(linearCombination(right, WrFull));
            sim = par.simMetric.sim(v1,v2);
            }
            similarityCache.put(hash, sim);
            return sim;
        }
    }
}
