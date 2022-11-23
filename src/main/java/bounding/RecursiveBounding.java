package bounding;

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
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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
    public static Boolean spark = true;
    public static JavaSparkContext sc;
    public static int i = 0;
    public static int level = 0;
    public List<ClusterCombination> ccs = new ArrayList<>();
    List<Cluster> Clusters = new ArrayList<>();
    public transient Map<Boolean, List<ClusterCombination>> dccs = new HashMap<>();


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
            //while (LHS.size() < par.maxPLeft || RHS.size() < par.maxPRight){
            if(spark) {
                while (i < 1) {
                    i++;

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
            if(!spark){
                while (LHS.size() < par.maxPLeft || RHS.size() < par.maxPRight){
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

    //    Get positive DCCs for a certain complexity
    public void assessComparisonTree(ClusterCombination rootCandidate, double shrinkFactor) {
        //        Make candidate list so that we can stream it
        //sc.close();
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf().setAppName("RB")
                .setMaster("local[16]").set("spark.executor.memory","32g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");
        StopWatch stopWatch = new StopWatch();

        List<ClusterCombination> rootCandidateList = new ArrayList<>();
        rootCandidateList.add(rootCandidate);
        Map<Boolean, List<ClusterCombination>> DCCs = new HashMap<>();
        if(!spark) {
            sc.close();
            stopWatch.start();
            System.out.println("Java starting....");
            DCCs = lib.getStream(rootCandidateList, par.parallel)
                    .unordered()
                    .flatMap(cc -> lib.getStream(recursiveBounding(cc, shrinkFactor, par), par.parallel))
                    .filter(dcc -> dcc.getCriticalShrinkFactor() <= 1)
                    .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
            stopWatch.stop();
            //System.out.println(DCCs);
            System.out.println("Java RB Time: " + stopWatch.getTime());
        }
        else {
            this.level++;
            System.out.println("spark starting....");
            JavaRDD<Cluster> rdd = sc.parallelize(Clusters, 16);
            //int max_results = 10000;
            if (this.level == 1) {
                //stopWatch.reset();
                stopWatch.start();

                for (int i = 0; i < clusterTree.size(); i++) {
                    for (int j = 0; j < clusterTree.get(i).size(); j++) {
                        //System.out.println(clusterTree.get(i).get(j).id);
                        Clusters.add(clusterTree.get(i).get(j));
                    }
                }

                //JavaRDD<Cluster> rdd = sc.parallelize(Clusters, 16);

                JavaRDD<ArrayList<Cluster>> rdd2;
                rdd2 = rdd.map((c1 -> {
                    ArrayList<Cluster> temp = new ArrayList<Cluster>(1);
                    temp.add(c1);
                    return temp;
                }));
                JavaPairRDD<ArrayList<Cluster>, ArrayList<Cluster>> cartesian = rdd2.cartesian(rdd2);
                cartesian = cartesian.filter(c1 -> !c1._1.equals(c1._2));
                JavaRDD<ClusterCombination> rdd3 = cartesian.map((c1 -> {
                    ClusterCombination cc = new ClusterCombination(c1._1, c1._2, 0);
                    return cc;
                }));
                rdd3 = rdd3.flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).iterator());
                rdd3 = rdd3.filter(dcc -> dcc.isPositive);
                rdd3 = rdd3.filter(dcc -> dcc.getCriticalShrinkFactor() <= 1);
                //ccs = rdd3.take(max_results);
                ccs = rdd3.collect();
                //Map<Boolean, List<ClusterCombination>> dccs = new HashMap<>();
                //dccs = rdd3.take(max_results).stream().collect(Collectors.partitioningBy(ClusterCombination::isPositive));
                dccs = ccs.stream().collect(Collectors.partitioningBy(ClusterCombination::isPositive));
                DCCs = dccs;
                stopWatch.stop();
                System.out.println("Spark RB Time: " + stopWatch.getTime());
                this.level++;
            }
            if (this.level > 1) {
                for(int i = 0; i < par.maxPRight; i++) {
                    stopWatch.reset();
                    stopWatch.start();
                    for (int j = 0; j < Clusters.size(); j++) {
                        for (int k = 0; k < dccs.get(false).size(); k++) {
                            if (Clusters.get(j) == dccs.get(false).get(k).getClusters().get(1)) {
                                Clusters.remove(Clusters.get(j));
                            }
                        }

                    }

                    JavaRDD<ClusterCombination> rdd2 = sc.parallelize(ccs);

                    JavaPairRDD<ClusterCombination, Cluster> cartesian = rdd2.cartesian(rdd);
                    JavaRDD<ClusterCombination> rdd3 = cartesian.map((c1 -> {
                        ArrayList<Cluster> LHS = new ArrayList<>();
                        LHS = c1._1.LHS;
                        ArrayList<Cluster> RHS = new ArrayList<>();
                        RHS.add(c1._1.RHS.get(0));
                        for (int j = 0; j < this.level - 1; j++) {
                            RHS.add(c1._2);
                        }
                        ClusterCombination cc = new ClusterCombination(LHS, RHS, 1);
                        return cc;
                    }));
                    rdd3 = rdd3.flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).iterator());
                    rdd3 = rdd3.filter(dcc -> dcc.isPositive);
                    rdd3 = rdd3.filter(dcc -> dcc.getCriticalShrinkFactor() <= 1);
                    //ccs = rdd3.take(10);
                    Map<Boolean, List<ClusterCombination>> dccs = new HashMap<>();
                    if(i == par.maxPRight - 1){
                        dccs = rdd3.collect().stream().collect(Collectors.partitioningBy(ClusterCombination::isPositive));
                        DCCs = dccs;
                    }
                }
                this.level++;
                //dccs = rdd3.take(max_results).stream().collect(Collectors.partitioningBy(ClusterCombination::isPositive));
                sc.close();
                stopWatch.stop();
                System.out.println("Spark RB Time: " + stopWatch.getTime());

            }
        }

        //System.out.println(DCCs);
//        Filter minJump confirming positives
        long start = System.nanoTime();
        this.positiveDCCs.addAll(unpackAndCheckMinJump(DCCs.get(true), par));
        /*postProcessTime += lib.nanoToSec(System.nanoTime() - start);

//        Sort (descending) and filter positive DCCs to comply to topK parameter
        if (par.topK > 0) {
            this.positiveDCCs = updateTopK(this.positiveDCCs, par);
        }

//        TODO SEE IF WE CAN MEASURE THIS TIME SEPARATELY
//        Handle negative DCCs using progressive approximation
        this.nNegDCCs.getAndAdd(DCCs.get(false).size());
        this.positiveDCCs = ProgressiveApproximation.ApproximateProgressively(DCCs.get(false), this.positiveDCCs, par, sc);*/
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

//            Get splitted CCs
            ArrayList<ClusterCombination> subCCs = CC.split(par.Wl.get(CC.LHS.size() - 1), par.Wr.size() > 0 ? par.Wr.get(CC.RHS.size() - 1): null, par.allowSideOverlap);
            if(spark){
                par.parallel = false;
            }else{
                return lib.getStream(subCCs, par.parallel).unordered()
                        .flatMap(subCC -> recursiveBounding(subCC, shrinkFactor, par).stream())
                        .collect(Collectors.toList());
            }
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