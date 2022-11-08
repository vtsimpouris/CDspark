package bounding;

import _aux.Pair;
import _aux.StatBag;
import core.Parameters;
import _aux.ResultTuple;
import _aux.lib;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;
import java.util.List;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;


@RequiredArgsConstructor
public class RecursiveBounding implements Serializable {

    @NonNull private Parameters par;
    @NonNull public ArrayList<ArrayList<Cluster>> clusterTree;

    public List<ClusterCombination> positiveDCCs = new ArrayList<>();
    public AtomicLong nNegDCCs = new AtomicLong(0);
    public double postProcessTime;
    public transient Set<ResultTuple> results;
    public static Boolean spark = false;
    public static JavaSparkContext sc;
    public static int i = 0;


    public Set<ResultTuple> run() {
        SparkConf sparkConf = new SparkConf().setAppName("RB")
                .setMaster("local[8]").set("spark.executor.memory","8g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        this.sc = sc;
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
                assessComparisonTree(rootCandidate, runningShrinkFactor, sc);

                //            Set shrink factor back to original value
                runningShrinkFactor = par.shrinkFactor;
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

//            Do pairwise with shrink factor 1
            assessComparisonTree(rootCandidate, 1, sc);

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
            assessComparisonTree(rootCandidate, par.shrinkFactor, sc);
        }

//        Set statistics
        par.statBag.addStat("nPosDCCs", positiveDCCs.size());
        par.statBag.addStat("nNegDCCs", nNegDCCs.get());
        par.statBag.addStat("postProcessTime", postProcessTime);

//        Convert to tuples

        this.results = positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
        return positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
    }

//    Get positive DCCs for a certain complexity
    public void assessComparisonTree(ClusterCombination rootCandidate, double shrinkFactor, JavaSparkContext sc) {
        //        Make candidate list so that we can stream it
        List<ClusterCombination> rootCandidateList = new ArrayList<>();
        rootCandidateList.add(rootCandidate);
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Map<Boolean, List<ClusterCombination>> DCCs = lib.getStream(rootCandidateList, par.parallel)
                .unordered()
                .flatMap(cc -> lib.getStream(recursiveBounding(cc, shrinkFactor, par), par.parallel))
                .filter(dcc -> dcc.getCriticalShrinkFactor() <= 1)
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
        //System.out.println("dccs:" + DCCs.toString());
        stopWatch.stop();
        System.out.println("Java RB Time: " + stopWatch.getTime());


        JavaRDD<ClusterCombination> rdd = sc.parallelize(recursiveBounding(rootCandidate, shrinkFactor, par));
        stopWatch.reset();
        stopWatch.start();
        //System.out.println("par sizes: " + rdd.count());

        rdd.map(subCC -> recursiveBounding(subCC, shrinkFactor, par));
        //System.out.println("result_spark " + rdd.collect());
        //List<ClusterCombination> subCCs = rdd.collect();
        Stream<ClusterCombination> stream = rdd.collect().stream();
        Map<Boolean, List<ClusterCombination>> dccs = stream.collect(Collectors.partitioningBy(y -> true));
        stopWatch.stop();
        System.out.println("spark RB Time: " + stopWatch.getTime());

        // Print out the total time of the watch


//        Filter minJump confirming positives
        long start = System.nanoTime();
        this.positiveDCCs.addAll(unpackAndCheckMinJump(DCCs.get(true), par));
        postProcessTime += lib.nanoToSec(System.nanoTime() - start);

//        Sort (descending) and filter positive DCCs to comply to topK parameter
        if (par.topK > 0) {
            this.positiveDCCs = updateTopK(this.positiveDCCs, par);
        }

//        TODO SEE IF WE CAN MEASURE THIS TIME SEPARATELY
//        Handle negative DCCs using progressive approximation
        this.nNegDCCs.getAndAdd(DCCs.get(false).size());
        this.positiveDCCs = ProgressiveApproximation.ApproximateProgressively(DCCs.get(false), this.positiveDCCs, par, sc);
    }

    //    TODO FIX WHAT HAPPENS FOR DISTANCES, WHERE YOU WANT EVERYTHING LOWER THAN A THRESHOLD

    public static List<ClusterCombination> recursiveBounding(ClusterCombination CC, double shrinkFactor, Parameters par) {
        ArrayList<ClusterCombination> DCCs = new ArrayList<>();

        double threshold = par.tau;

//        Get bounds
        CC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(CC.LHS.size() - 1), CC.RHS.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null,
                par.pairwiseDistances);

//      Update statistics
        //System.out.println("par.statBag.nCCs" + par.statBag.nCCs.set(1));
        par.statBag = new StatBag();
        par.statBag.nCCs = new AtomicLong(i);
        par.statBag.totalCCSize = new AtomicLong(CC.size());
        i++;
        //par.statBag.nCCs.getAndIncrement();
        //par.statBag.totalCCSize.addAndGet(CC.size());

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
            //System.out.println("subCCs: " + subCCs);


            //System.out.println(subCCs.toString());

            // this is in the wrong place


            /*List<ClusterCombination> s = new ArrayList<>();
            JavaRDD<ClusterCombination> rdd = sc.parallelize(subCCs);
            spark = true;
            if (spark && i < 80) {
                        rdd.map(subCC -> recursiveBounding(subCC, shrinkFactor, par, false));
                        System.out.println("result_spark " + rdd.collect());
                        rdd.collect();
            }

            spark = false;
            return rdd.collect();*/
            /*List<List<ClusterCombination>> s2 = new ArrayList<>();
            for(int i = 0; i < subCCs.size(); i++){
                s2.add(recursiveBounding(subCCs.get(i), shrinkFactor, par, false));
                System.out.println("result " + s2.get(i));
            }
            //System.out.println("\n");
            List<ClusterCombination> result = new ArrayList<>();
            s2.forEach(result::addAll);
            return result;*/

            /*List<List<ClusterCombination>> s2 = new ArrayList<>();
            for(int i = 0; i < subCCs.size(); i++){
                s2.add(recursiveBounding(subCCs.get(i), shrinkFactor, par));
                //System.out.println("result " + s2.get(i));
            }
            //System.out.println("\n");
            List<ClusterCombination> result = new ArrayList<>();
            s2.forEach(result::addAll);

            //System.out.println("result " + result);


            List<ClusterCombination> ccs = lib.getStream(subCCs, par.parallel).unordered()
                    .flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).stream())
                    .collect(Collectors.toList());
            //System.out.println("ccs: " + ccs);
            //System.out.println("ccs class: " + ccs.getClass());
            //System.out.println("\n");*/
            //return returned.collect();
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
        }
        return DCCs;
    }

    public static List<ClusterCombination> unpackAndCheckMinJump(List<ClusterCombination> positiveDCCs, Parameters par){
        List<ClusterCombination> out;

        out = lib.getStream(positiveDCCs, par.parallel).unordered()
                .flatMap(cc -> cc.getSingletons(par.Wl.get(cc.LHS.size() - 1), par.Wr.size() > 0 ? par.Wr.get(cc.RHS.size() - 1): null, par.allowSideOverlap).stream())
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

    public static List<ClusterCombination> updateTopK(List<ClusterCombination> positiveDCCs, Parameters par){
        //        Sort (descending) and filter positive DCCs to comply to topK parameter
        if (positiveDCCs.size() > par.topK){
            positiveDCCs = lib.getStream(positiveDCCs, par.parallel)
                    .sorted((cc1, cc2) -> Double.compare(cc2.getLB(), cc1.getLB()))
                    .limit(par.topK)
                    .collect(Collectors.toList());

//            Update correlation threshold
            par.tau = Math.max(par.tau, positiveDCCs.get(positiveDCCs.size()-1).getLB());

            par.LOGGER.fine("TopK reached. New correlation threshold: " + par.tau);
        }
        return positiveDCCs;

    }


}
