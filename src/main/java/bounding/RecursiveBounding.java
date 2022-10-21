package bounding;

import core.Parameters;
import _aux.ResultTuple;
import _aux.lib;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecursiveBounding {

    @NonNull private Parameters par;
    @NonNull private ArrayList<ArrayList<Cluster>> clusterTree;

    public List<ClusterCombination> positiveDCCs = new ArrayList<>();
    public AtomicLong nCCs = new AtomicLong(0);
    public AtomicLong totalCCSize = new AtomicLong(0);

    public Set<ResultTuple> run() {
        double postProcessTime = 0;

        Cluster rootCluster = clusterTree.get(0).get(0);

//        TODO BUILD IN PROGRESSIVE COMPLEXITY BOUNDING (I.E. (1,1) -> (2,1), (1,2) -> (3,1), (2,2), (1,3) -> (4,1), (1,4), (3,2), (2,3)

//        ------------------- STAGE 1 BOUND PAIRWISE ---------------------------------
//        First compute all pairwise cluster bounds to fill cache and increase threshold in case of topK
        ArrayList<Cluster> rootLeft = new ArrayList<>();
        ArrayList<Cluster> rootRight = new ArrayList<>();
        rootLeft.add(rootCluster);

        if (par.simMetric.isTwoSided()){
            rootRight.add(rootCluster);
        } else {
            rootLeft.add(rootCluster);
        }

        ClusterCombination pairwiseRootCandidate = new ClusterCombination(rootLeft, rootRight, 0);

        Map<Boolean, List<ClusterCombination>> pairwiseDCCs = recursiveBounding(pairwiseRootCandidate)
                .stream().collect(Collectors.partitioningBy(ClusterCombination::isPositive));

        long start = System.nanoTime();

//        Filter pairwise posDCCs
        List<ClusterCombination> positivePairwiseDCCs = unpackAndCheckMinJump(pairwiseDCCs.get(true), par);
        postProcessTime += lib.nanoToSec(System.nanoTime() - start);


//        Pair<List<ClusterCombination>, List<ClusterCombination>> pairwiseDCCs = getAndFilterDCCs(pairwiseRootCandidate);

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

        ClusterCombination rootCandidate = new ClusterCombination(rootLeft, rootRight, 0);

        //        Make candidate list so that we can stream it
        List<ClusterCombination> rootCandidateList = new ArrayList<>(); rootCandidateList.add(rootCandidate);

        Map<Boolean, List<ClusterCombination>> DCCs = lib.getStream(rootCandidateList, par.parallel)
                .unordered()
                .flatMap(cc -> lib.getStream(recursiveBounding(cc), par.parallel))
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));

//        Filter minJump confirming positives
        List<ClusterCombination> positiveDCCs = DCCs.get(true);

        start = System.nanoTime();
        this.positiveDCCs = unpackAndCheckMinJump(positiveDCCs, par);
        postProcessTime += lib.nanoToSec(System.nanoTime() - start);

//        TODO FILTER TOPK
//        TODO PROGRESSIVE APPROXIMATION

//        Get final DCCs
        this.positiveDCCs.addAll(positivePairwiseDCCs);

        List<ClusterCombination> negativeDCCs = DCCs.get(false);
        negativeDCCs.addAll(pairwiseDCCs.get(false));

//        Set statistics
        par.statBag.addStat("nPosDCCs", positiveDCCs.size());
        par.statBag.addStat("nNegDCCs", negativeDCCs.size());
        par.statBag.addStat("nDCCs", positiveDCCs.size() + negativeDCCs.size());
        par.statBag.addStat("postProcessTime", postProcessTime);

//        Convert to tuples
        return positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
    }

//    TODO FIX WHAT HAPPENS FOR DISTANCES, WHERE YOU WANT EVERYTHING LOWER THAN A THRESHOLD
    public List<ClusterCombination> recursiveBounding(ClusterCombination CC) {
        ArrayList<ClusterCombination> DCCs = new ArrayList<>();

        double threshold = par.tau;

//        Get bounds
        CC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(CC.LHS.size() - 1), CC.RHS.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null,
                par.pairwiseDistances);

//      Update statistics
        nCCs.incrementAndGet();
        totalCCSize.addAndGet(CC.size());

//        Update threshold based on minJump if we have CC > 2
        double jumpBasedThreshold = CC.getMaxPairwiseLB() + par.minJump;
        if (CC.LHS.size() + CC.RHS.size() > 2){
            threshold = Math.max(threshold, jumpBasedThreshold);
        }

//        Check if CC is (in)decisive
        if ((CC.getLB() < threshold) && (CC.getUB() >= threshold)){
            CC.setDecisive(false);

//            Get splitted CCs
            ArrayList<ClusterCombination> subCCs = CC.split(par.Wl.get(CC.LHS.size() - 1), par.Wr.get(CC.RHS.size() - 1), par.allowSideOverlap);

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
                .flatMap(cc -> cc.getSingletons(par.Wl.get(cc.LHS.size() - 1), par.Wr.get(cc.RHS.size() - 1), par.allowSideOverlap).stream())
                .filter(cc -> {
                    cc.bound(par.simMetric, par.empiricalBounding, par.Wl.get(cc.LHS.size() - 1),
                            cc.RHS.size() > 0 ? par.Wr.get(cc.RHS.size() - 1): null, par.pairwiseDistances);
                    if (Math.abs(cc.getLB() - cc.getUB()) > 0.001) {
                        par.LOGGER.info("postprocessing: found a singleton CC with LB != UB");
                        return false;
                    }
                    return (cc.getMaxSubsetSimilarity(par) + par.minJump) < cc.getLB();
                })
                .collect(Collectors.toList());
        return out;

    }


}
