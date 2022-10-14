package bounding;

import _aux.Pair;
import _aux.Parameters;
import _aux.ResultTuple;
import _aux.lib;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecursiveBounding {

    @NonNull private Parameters par;
    @NonNull private ArrayList<ArrayList<Cluster>> clusterTree;

    public List<ResultTuple> run() {
        Cluster rootCluster = clusterTree.get(0).get(0);

//        ------------------- STAGE 1 BOUND PAIRWISE ---------------------------------
//        First compute all pairwise cluster bounds to fill cache and increase threshold in case of topK
        ArrayList<Cluster> rootLeft = new ArrayList<>();
        rootLeft.add(rootCluster);
        ArrayList<Cluster> rootRight = new ArrayList<>();
        rootRight.add(rootCluster);

        ClusterCombination pairwiseRootCandidate = new ClusterCombination(rootLeft, rootRight);

        Pair<List<ClusterCombination>, List<ClusterCombination>> pairwiseDCCs = getAndFilterDCCs(pairwiseRootCandidate);

//        TODO FILTER TOPK

//        ------------------- STAGE 2 BOUND HIGH-ORDER COMBINATIONS ---------------------------------
//        Now compute all high-order cluster bounds
        rootLeft = new ArrayList<>();
        for (int i = 0; i < par.maxPLeft; i++) {
            rootLeft.add(rootCluster);
        }

        rootRight = new ArrayList<>();
        for (int i = 0; i < par.maxPRight; i++) {
            rootRight.add(rootCluster);
        }

        ClusterCombination rootCandidate = new ClusterCombination(rootLeft, rootRight);

        Pair<List<ClusterCombination>, List<ClusterCombination>> DCCs = getAndFilterDCCs(rootCandidate);

//        TODO FILTER TOPK
//        TODO PROGRESSIVE APPROXIMATION

//        Get final positive DCCs
        List<ClusterCombination> positiveDCCs = DCCs.x;
        positiveDCCs.addAll(pairwiseDCCs.x);

//        Convert to tuples
        return positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toList());
    }

//    Start bounding from root candidate and filter positives based on minJump - output: <posDCCs, negDCCs>
    public Pair<List<ClusterCombination>, List<ClusterCombination>> getAndFilterDCCs(ClusterCombination rootCandidate){

        //        Make candidate list so that we can stream it
        List<ClusterCombination> rootCandidateList = new ArrayList<>(); rootCandidateList.add(rootCandidate);

        Map<Boolean, List<ClusterCombination>> DCCs = lib.getStream(rootCandidateList, par.parallel)
                .unordered()
                .flatMap(cc -> lib.getStream(recursiveBounding(cc), par.parallel))
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));

//        Filter minJump confirming positives
        List<ClusterCombination> posDCCs = DCCs.get(true);
        posDCCs = unpackAndCheckMinJump(posDCCs, par);

        return new Pair<>(posDCCs, DCCs.get(false));
    }

//    TODO FIX WHAT HAPPENS FOR DISTANCES, WHERE YOU WANT EVERYTHING LOWER THAN A THRESHOLD
    public List<ClusterCombination> recursiveBounding(ClusterCombination CC) {
        ArrayList<ClusterCombination> DCCs = new ArrayList<>();

        double threshold = par.tau;

//        Get bounds
        CC.bound(par.simMetric, par.empiricalBounding, par.Wl, par.Wr, par.pairwiseDistances);

//        Update threshold based on minJump if we have CC > 2
        double jumpBasedThreshold = CC.getMaxLowerBoundSubset() + par.minJump;
        if (CC.LHS.size() + CC.RHS.size() > 2){
            threshold = Math.max(threshold, jumpBasedThreshold);
        }

//        Check if CC is (in)decisive
        if ((CC.getLB() < threshold) && (CC.getUB() >= threshold)){
            CC.setDecisive(false);

            ArrayList<ClusterCombination> subCCs = CC.split();

            return lib.getStream(subCCs, par.parallel).unordered()
                    .flatMap(subCC -> recursiveBounding(subCC).stream())
                    .collect(Collectors.toList());
        } else { // CC is decisive, add to DCCs
            CC.setDecisive(true);

            CC.setPositive(CC.getLB() >= threshold);
            DCCs.add(CC);
        }
        return DCCs;
    }

    public static List<ClusterCombination> unpackAndCheckMinJump(List<ClusterCombination> positiveDCCs, Parameters par){
        List<ClusterCombination> out;

        out = lib.getStream(positiveDCCs, par.parallel).unordered()
                .flatMap(cc -> cc.getSingletons().stream())
                .filter(cc -> { // remove cases where LHS and RHS overlap
                    for(Cluster c : cc.LHS){
                        if(cc.RHS.contains(c)){
                            return false;
                        }
                    }
                    return true;
                })
                .filter(cc -> {
                    cc.bound(par.simMetric, par.empiricalBounding, par.Wl, par.Wr, par.pairwiseDistances);
                    if (Math.abs(cc.getLB() - cc.getUB()) > 0.001) {
                        par.LOGGER.info("postprocessing: found a singleton CC with LB != UB");
                    }
                    return (cc.getMaxSubsetSimilarity(par) + par.minJump) < cc.getLB();
                })
                .collect(Collectors.toList());
        return out;

    }


}
