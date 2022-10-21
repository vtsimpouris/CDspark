package algorithms.baselines;

import _aux.lib;
import core.Parameters;

import java.util.List;

public class SimpleBaseline extends Baseline{
    public SimpleBaseline(Parameters par) {
        super(par);
    }

    @Override public void prepare(){}

//    Compute similarities exhaustively
    @Override
    public double computeSimilarity(List<Integer> left, List<Integer> right){
        long hash = lib.hashTwoLists(left, right);
        if(similarityCache.containsKey(hash)){
            return similarityCache.get(hash);
        }
        else{
//            Make linear combinations
            double[] v1 = par.simMetric.preprocess(linearCombination(left, WlFull));
            double[] v2 = par.simMetric.preprocess(linearCombination(right, WrFull));
            double sim = par.simMetric.sim(v1,v2);
            similarityCache.put(hash, sim);
            return sim;
        }
    }
}
