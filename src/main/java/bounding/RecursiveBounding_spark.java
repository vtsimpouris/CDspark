package bounding;

import _aux.StatBag;
import core.Parameters;
import _aux.ResultTuple;
import _aux.lib;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class RecursiveBounding_spark implements Serializable {

    @NonNull private Parameters par;
    @NonNull public ArrayList<ArrayList<Cluster>> clusterTree;

    public List<ClusterCombination> positiveDCCs = new ArrayList<>();
    public AtomicLong nNegDCCs = new AtomicLong(0);
    public double postProcessTime;
    public transient Set<ResultTuple> results;

    public Set<ResultTuple> run() {

        Cluster rootCluster = clusterTree.get(0).get(0);
        //        Make initial cluster comparison
//        Progressively build up complexity from (1,1) to (maxPLeft,maxPRight) and get all DCCs with complexity <= (maxPLeft,maxPRight) (unless custom aggregation)
//        I.e. if query = mc(2,4); (1,1) -> (1,2) -> (2,2) -> (2,3) -> (2,4)
        if(!par.aggPattern.contains("custom")){
//            Setup first iteration
            ArrayList<Cluster> LHS = new ArrayList<>(Arrays.asList(rootCluster));
            ArrayList<Cluster> RHS = new ArrayList<>();

//            Do first iteration with shrinkFactor 1
            double runningShrinkFactor = 1;

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
                assessComparisonTree(rootCandidate, runningShrinkFactor);

                //            Set shrink factor back to original value
                runningShrinkFactor = par.shrinkFactor;
            }
        }
        this.results = positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
        return positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
    }

    public void assessComparisonTree(ClusterCombination rootCandidate, double shrinkFactor) {
        //        Make candidate list so that we can stream it
        List<ClusterCombination> rootCandidateList = new ArrayList<>(); rootCandidateList.add(rootCandidate);

        Map<Boolean, List<ClusterCombination>> DCCs = lib.getStream(rootCandidateList, par.parallel)
                .unordered()
                .flatMap(cc -> lib.getStream(recursiveBounding(cc, shrinkFactor, par), par.parallel))
                .filter(dcc -> dcc.getCriticalShrinkFactor() <= 1)
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));

//        Filter minJump confirming positives
        long start = System.nanoTime();
        /*this.positiveDCCs.addAll(unpackAndCheckMinJump(DCCs.get(true), par));
        postProcessTime += lib.nanoToSec(System.nanoTime() - start);

//        Sort (descending) and filter positive DCCs to comply to topK parameter
        if (par.topK > 0) {
            this.positiveDCCs = updateTopK(this.positiveDCCs, par);
        }*/

//        TODO SEE IF WE CAN MEASURE THIS TIME SEPARATELY
//        Handle negative DCCs using progressive approximation
        this.nNegDCCs.getAndAdd(DCCs.get(false).size());
        this.positiveDCCs = ProgressiveApproximation.ApproximateProgressively(DCCs.get(false), this.positiveDCCs, par);
    }
    public static List<ClusterCombination> recursiveBounding(ClusterCombination CC, double shrinkFactor, Parameters par) {
        ArrayList<ClusterCombination> DCCs = new ArrayList<>();

        double threshold = par.tau;

//        Get bounds
        CC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(CC.LHS.size() - 1), CC.RHS.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null,
                par.pairwiseDistances);

//      Update statistics

        System.out.println(par.statBag);
        //par.statBag.nCCs.getAndIncrement();
        /*par.statBag.totalCCSize.addAndGet(CC.size());

//        Shrink upper bound for progressive bounding
        double shrunkUB = CC.getShrunkUB(shrinkFactor, par.maxApproximationSize);

//        Update threshold based on minJump if we have CC > 2
        double jumpBasedThreshold = CC.getMaxPairwiseLB() + par.minJump;
        if (CC.LHS.size() + CC.RHS.size() > 2){
            threshold = Math.max(threshold, jumpBasedThreshold);
        }

//        Check if CC is (in)decisive
        if ((CC.getLB() < threshold) && (shrunkUB >= threshold)){
            CC.setDecisive(false);

//            Get splitted CCs
            ArrayList<ClusterCombination> subCCs = CC.split(par.Wl.get(CC.LHS.size() - 1), par.Wr.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null, par.allowSideOverlap);

            return lib.getStream(subCCs, par.parallel).unordered()
                    .flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).stream())
                    .collect(Collectors.toList());
        } else { // CC is decisive, add to DCCs
            CC.setDecisive(true);

//            Negative DCC, set critical shrink factor in order to investigate later when using progressive approximation
            if (shrunkUB < threshold) {
                CC.setCriticalShrinkFactor(threshold);
                if (CC.getCriticalShrinkFactor() <= 1 && threshold <= 1) {
                    DCCs.add(CC);
                }
            } else if (CC.getLB() >= threshold){ //  Positive DCC
                CC.setPositive(true);
                CC.criticalShrinkFactor = -10;
                DCCs.add(CC);
            }
        }*/
        return DCCs;
    }

    //    Get positive DCCs for a certain complexit

}
