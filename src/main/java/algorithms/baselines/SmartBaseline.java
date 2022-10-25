package algorithms.baselines;

import _aux.Pair;
import _aux.ResultTuple;
import bounding.ClusterCombination;
import core.Parameters;
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

    @Override
    public boolean assessCandidate(Pair<List<Integer>, List<Integer>> candidate){
//        Get bounded singleton cc
        ClusterCombination cc = computeSimilarity(candidate.x, candidate.y);
        double sim = cc.getLB();

//        If significant also check minJump
        if (sim > par.tau){
            if (par.minJump > 0){
                double maxSubsetSim = cc.getMaxSubsetSimilarity(par);
                if (maxSubsetSim + par.minJump > sim){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

//    Compute similarities based on pairwise similarities (if possible)
    public ClusterCombination computeSimilarity(List<Integer> left, List<Integer> right){
//        Create cluster combination
        ArrayList<Cluster> LHS = left.stream().map(i -> singletonClusters[i]).collect(Collectors.toCollection(ArrayList::new));
        ArrayList<Cluster> RHS = right.stream().map(i -> singletonClusters[i]).collect(Collectors.toCollection(ArrayList::new));
        ClusterCombination cc = new ClusterCombination(LHS, RHS, 0);
        cc.bound(par.simMetric, true,
                par.Wl.get(left.size() - 1), par.Wr.size() > 0 ? par.Wr.get(right.size() - 1): null,
                par.pairwiseDistances);
        return cc;
    }
}
