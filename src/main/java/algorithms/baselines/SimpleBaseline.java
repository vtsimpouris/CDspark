package algorithms.baselines;

import _aux.Pair;
import _aux.lib;
import bounding.ClusterCombination;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleBaseline extends Baseline{
    @RequiredArgsConstructor
    private class SimCacheValue{
        @NonNull public List<Integer> left;
        @NonNull public List<Integer> right;
        @NonNull public double sim;
    }

    ConcurrentHashMap<Long, SimCacheValue> simCache = new ConcurrentHashMap<>();

    public SimpleBaseline(Parameters par) {
        super(par);
    }

    @Override public void prepare(){}

    @Override
    public boolean assessCandidate(Pair<List<Integer>, List<Integer>> candidate){
//        Get bounded singleton cc
        double sim = computeSimilarity(candidate.x, candidate.y);

//        If significant also check minJump
        if (sim > par.tau){
            if (par.minJump > 0){
                double maxSubsetSim = getMaxSubsetSimilarty(candidate.x, candidate.y);
                if (maxSubsetSim + par.minJump > sim){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

//    Compute similarities exhaustively
    private double computeSimilarity(List<Integer> left, List<Integer> right){
        long hash = hashCandidate(left,right);

        if (simCache.containsKey(hash)){
            SimCacheValue val = simCache.get(hash);
            if (val.left.equals(left) && val.right.equals(right)){
                return val.sim;
            }
        }
        double[] v1 = par.simMetric.preprocess(linearCombination(left, WlFull));
        double[] v2 = par.simMetric.preprocess(linearCombination(right, WrFull));
        double sim = par.simMetric.sim(v1,v2);
        simCache.put(hash, new SimCacheValue(left, right, sim));
        return sim;
    }

    private double getMaxSubsetSimilarty(List<Integer> left, List<Integer> right){
        double maxSim = 0;

//        First LHS
        if (left.size() > 1){
            for (int i = 0; i < left.size(); i++){
                List<Integer> leftSubset = new ArrayList<>(left);
                leftSubset.remove((int) i);
                double sim = computeSimilarity(leftSubset, right);
                if (sim > maxSim){
                    maxSim = sim;
                }
            }
        }

//        Then RHS
        if (right.size() > 1){
            for (int i = 0; i < right.size(); i++){
                List<Integer> rightSubset = new ArrayList<>(right);
                rightSubset.remove((int) i);
                double sim = computeSimilarity(left, rightSubset);
                if (sim > maxSim){
                    maxSim = sim;
                }
            }
        }

        return maxSim;
    }

    public long hashCandidate(List<Integer> left, List<Integer> right){
        List<Integer> hashList = new ArrayList<>(left);
        hashList.add(-1);
        hashList.addAll(right);
        return hashList.hashCode();
    }
}
