package bounding;

import _aux.StatBag;
import core.Parameters;
import _aux.ResultTuple;
import _aux.lib;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

@RequiredArgsConstructor
public class RecursiveBounding implements Serializable {

    @NonNull private Parameters par;
    @NonNull public ArrayList<ArrayList<Cluster>> clusterTree;

    public List<ClusterCombination> positiveDCCs = new ArrayList<>();
    public AtomicLong nNegDCCs = new AtomicLong(0);
    public double postProcessTime;
    public transient Set<ResultTuple> results;
    public static JavaSparkContext sc;
    public static int i = 0;
    public static int level = 0;
    public List<ClusterCombination> ccs = new ArrayList<>();
    List<ArrayList<Cluster>> Clusters = new ArrayList<ArrayList<Cluster>>();
    public transient Map<Boolean, List<ClusterCombination>> dccs = new HashMap<>();
    public static int executors = 0;
    public static int max_executors = 16;


    public Set<ResultTuple> run() {
        //this.sc = sc;
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
            int i = 0;
            while (LHS.size() < par.maxPLeft || RHS.size() < par.maxPRight){

//                Make new lists to avoid concurrent modification
                    LHS = new ArrayList<>(LHS);
                    RHS = new ArrayList<>(RHS);

                    if ((LHS.size() == RHS.size() && LHS.size() < par.maxPLeft) || RHS.size() == par.maxPRight) { // always increase LHS first
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
            assessComparisonTree(rootCandidate, 1);

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
            assessComparisonTree(rootCandidate, par.shrinkFactor);
        }

//        Set statistics
        par.statBag.addStat("nPosDCCs", positiveDCCs.size());
        par.statBag.addStat("nNegDCCs", nNegDCCs.get());
        par.statBag.addStat("postProcessTime", postProcessTime);

//        Convert to tuples

        this.results = positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
        return positiveDCCs.stream().map(cc -> cc.toResultTuple(par.headers)).collect(Collectors.toSet());
    }
    public static class Element implements Comparable<Element> {
        int index;
        Double value;
        Element(int index, Double value){
            this.index = index;
            this.value = value;
        }
        public int compareTo(Element e) {
            return Double.compare(this.value, e.value);
        }
    }
    public static class SubCCs{
        List<ClusterCombination> bigSubCCs = new ArrayList<ClusterCombination>();
        List<ClusterCombination> smallSubCCs = new ArrayList<ClusterCombination>();
    }

    public static SubCCs splitSubCCs(ArrayList<ClusterCombination> subCCs){
        List<Element> radiusList = new ArrayList<Element>();

        for(int i = 0; i < subCCs.size(); i++) {
                radiusList.add(new Element(i,subCCs.get(i).getClusters().get(0).getRadius()));
        }
        Collections.sort(radiusList);
        Collections.reverse(radiusList); // If you want reverse order
        /*for (Element element : radiusList) {
                System.out.println(element.value + " " +
                        "" + element.index);
        }*/
        SubCCs allSubCCs = new SubCCs();
        for (Element element : radiusList) {
            if(executors > max_executors){
                allSubCCs.smallSubCCs.add(subCCs.get(element.index));
            }else{
                allSubCCs.bigSubCCs.add(subCCs.get(element.index));
            }
            executors++;
        }
        return allSubCCs;
    }

    // wrapper function to open up spark parallelism
    public static List<ClusterCombination> recursiveBounding_spark(ClusterCombination CC, double shrinkFactor, Parameters par) {
        ArrayList<ClusterCombination> DCCs = new ArrayList<>();

        double threshold = par.tau;

//        Get bounds
        CC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(CC.LHS.size() - 1), CC.RHS.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null,
                par.pairwiseDistances);

//      Update statistics
        //System.out.println("LHS: " + CC.LHS + "RHS: " + CC.RHS);
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

            // Split CCs with biggest radius for spark computation. Smallest clusters are send for local computation.
            SubCCs allSubCCs = splitSubCCs(subCCs);
            ArrayList<ClusterCombination> bigSubCCs = new ArrayList<ClusterCombination>(allSubCCs.bigSubCCs);
            ArrayList<ClusterCombination> smallSubCCs = new ArrayList<ClusterCombination>(allSubCCs.smallSubCCs);
            List<ClusterCombination> spark_CCs = new ArrayList<ClusterCombination>();
            List<ClusterCombination> local_CCs = new ArrayList<ClusterCombination>();

            // spark computation
            JavaRDD<ClusterCombination> rdd = sc.parallelize(bigSubCCs,max_executors);
            rdd = rdd.flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).iterator());

            // local computation
            par.parallel = true;
            local_CCs = lib.getStream(smallSubCCs, par.parallel).unordered()
                                .flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).stream())
                                .collect(Collectors.toList());
            spark_CCs = rdd.collect();
            // merge results
            List<ClusterCombination> CCs = Stream.concat(spark_CCs.stream().parallel(), local_CCs.stream().parallel())
                    .collect(Collectors.toList());
            return CCs;


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
    public Map<Boolean, List<ClusterCombination>> sparkRB(ClusterCombination rootCandidate, double shrinkFactor) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf().setAppName("RB")
                .setMaster("local[16]").set("spark.executor.memory","16g").set("spark.driver.maxResultSize", "4g");
        // start a spark context
        sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        List<ClusterCombination> ccs_spark = new ArrayList<>();
        ccs_spark =  recursiveBounding_spark(rootCandidate, shrinkFactor, par);
        sc.close();
        return ccs_spark
                .stream()
                .filter(dcc -> dcc.getCriticalShrinkFactor() <= 1)
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
    }

    //    Get positive DCCs for a certain complexity
    public void assessComparisonTree(ClusterCombination rootCandidate, double shrinkFactor) {

        //        Make candidate list so that we can stream it
        List<ClusterCombination> rootCandidateList = new ArrayList<>();
        rootCandidateList.add(rootCandidate);
        Map<Boolean, List<ClusterCombination>> DCCs = new HashMap<>();
        //System.out.println("Java starting....");


        if(par.java) {
            //System.out.println(par.parallel);
            DCCs = lib.getStream(rootCandidateList, par.parallel)
                    .unordered()
                    .flatMap(cc -> lib.getStream(recursiveBounding(cc, shrinkFactor, par), par.parallel))
                    .filter(dcc -> dcc.getCriticalShrinkFactor() <= 1)
                    .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
        }
        if(par.spark) {
            DCCs = sparkRB(rootCandidate, shrinkFactor);
        }

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
        //System.out.println("LHS: " + CC.LHS + "RHS: " + CC.RHS);
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
            if(par.spark){
                par.parallel=false;
            }

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
        }
        return DCCs;
    }

    public static List<ClusterCombination> unpackAndCheckMinJump(List<ClusterCombination> positiveDCCs, Parameters par){
        List<ClusterCombination> out;
        if(par.spark) {
            par.parallel = true;
        }
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
        if(par.spark) {
            par.parallel = true;
        }
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