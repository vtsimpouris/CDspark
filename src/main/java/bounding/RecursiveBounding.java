package bounding;

import core.Parameters;
import _aux.ResultTuple;
import _aux.lib;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecursiveBounding {

    @NonNull private Parameters par;
    @NonNull private ArrayList<ArrayList<Cluster>> clusterTree;

    public List<ClusterCombination> positiveDCCs = new ArrayList<>();
    public AtomicLong nCCs = new AtomicLong(0);
    public AtomicLong nNegDCCs = new AtomicLong(0);
    public AtomicLong totalCCSize = new AtomicLong(0);
    public double postProcessTime;

    public Set<ResultTuple> run() {

        Cluster rootCluster = clusterTree.get(0).get(0);

//        Make initial cluster comparison
//        Progressively build up complexity from (1,1) to (maxPLeft,maxPRight) and get all DCCs with complexity <= (maxPLeft,maxPRight) (unless custom aggregation)
//        I.e. if query = mc(2,4); (1,1) -> (1,2) -> (2,2) -> (2,3) -> (2,4)
        if(!par.aggPattern.contains("custom")){
//            Setup first iteration
            ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(rootCluster));
            ArrayList<Cluster> RHS = new ArrayList<>();

            while (LHS.size() < par.maxPLeft || RHS.size() < par.maxPRight){
//                Make new lists to avoid concurrent modification
                LHS = new ArrayList<>(LHS);
                RHS = new ArrayList<>(RHS);

                if ((LHS.size() == RHS.size() && LHS.size() < par.maxPLeft) || RHS.size() == par.maxPRight){ // always increase LHS first
                    LHS.add(rootCluster);
                } else {
                    RHS.add(rootCluster);
                }
                ClusterCombination rootCandidate = new ClusterCombination(LHS, RHS, 0);
                this.positiveDCCs.addAll(startRecursiveBounding(rootCandidate));
            }
        }
//        Do pairwise and max complexity
        else{
            //        First compute all pairwise cluster bounds to fill cache and increase threshold in case of topK
            ArrayList<Cluster> rootLeft = new ArrayList<>();
            ArrayList<Cluster> rootRight = new ArrayList<>();
            rootLeft.add(rootCluster);

            if (par.simMetric.isTwoSided()){
                rootRight.add(rootCluster);
            } else {
                rootLeft.add(rootCluster);
            }
            ClusterCombination rootCandidate = new ClusterCombination(rootLeft, rootRight, 0);
            this.positiveDCCs.addAll(startRecursiveBounding(rootCandidate));

//        Now compute the high-order DCCs
            rootLeft = new ArrayList<>();
            for (int i = 0; i < par.maxPLeft; i++) {
                rootLeft.add(rootCluster);
            }

            rootRight = new ArrayList<>();
            for (int i = 0; i < par.maxPRight; i++) {
                rootRight.add(rootCluster);
            }

            rootCandidate = new ClusterCombination(rootLeft, rootRight, 0);
            this.positiveDCCs.addAll(startRecursiveBounding(rootCandidate));
        }

//        Set statistics
        par.statBag.addStat("nPosDCCs", positiveDCCs.size());
        par.statBag.addStat("nNegDCCs", nNegDCCs.get());
        par.statBag.addStat("postProcessTime", postProcessTime);

//        Convert to tuples
        return positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
    }

//    Get positive DCCs for a certain complexity
    public List<ClusterCombination> startRecursiveBounding(ClusterCombination rootCandidate) {
        //        Make candidate list so that we can stream it
        List<ClusterCombination> rootCandidateList = new ArrayList<>(); rootCandidateList.add(rootCandidate);

        Map<Boolean, List<ClusterCombination>> DCCs = lib.getStream(rootCandidateList, par.parallel)
                .unordered()
                .flatMap(cc -> lib.getStream(recursiveBounding(cc), par.parallel))
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));

//        Filter minJump confirming positives
        long start = System.nanoTime();
        List<ClusterCombination> positiveDCCs = unpackAndCheckMinJump(DCCs.get(true), par);
        postProcessTime += lib.nanoToSec(System.nanoTime() - start);

//        TODO FILTER TOPK
//        TODO PROGRESSIVE APPROXIMATION
//        Handle negative DCCs
        this.nNegDCCs.getAndAdd(DCCs.get(false).size());


        return positiveDCCs;
    }

    //    TODO FIX WHAT HAPPENS FOR DISTANCES, WHERE YOU WANT EVERYTHING LOWER THAN A THRESHOLD
    public List<ClusterCombination> recursiveBounding(ClusterCombination CC) {
        ArrayList<ClusterCombination> DCCs = new ArrayList<>();

        double threshold = par.tau;

//        Get bounds
        CC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(CC.LHS.size() - 1), CC.RHS.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null,
                par.pairwiseDistances);

//      Update statistics
        nCCs.getAndIncrement();
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
