package bounding;

import _aux.StatBag;
import _aux.lib;
import clustering.Cluster;
import core.Parameters;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class SparkBounding {
    @NonNull private Parameters par;
    @NonNull public ArrayList<ArrayList<Cluster>> clusterTree;
    public static int i = 0;
    public static int slices = 16;


    public void run(){
        Cluster rootCluster = clusterTree.get(0).get(0);


        ArrayList<Cluster> LHS = new ArrayList<>();
        ArrayList<Cluster> RHS = new ArrayList<>();
        ArrayList<ClusterCombination> CCs = new ArrayList<>();
        for(int i = 0; i < clusterTree.size(); i++){
            for(int j = 0; j < clusterTree.get(i).size(); j++){
                LHS.add(clusterTree.get(i).get(j));
                RHS.add(clusterTree.get(i).get(j));
                ClusterCombination Candidates = new ClusterCombination(LHS, RHS, 0);
                CCs.add(Candidates);
            }
        }
        //System.out.println(Arrays.toString(LHS.toArray()));
        //System.out.println(Arrays.toString(RHS.toArray()));

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf().setAppName("RB")
                .setMaster("local[16]").set("spark.executor.memory","16g").set("spark.driver.maxResultSize", "4g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        JavaRDD<ClusterCombination> rdd = sc.parallelize(CCs,slices);
        JavaPairRDD<ClusterCombination, ClusterCombination> rdd1 = rdd.cartesian(rdd);
        rdd.collect();
        sc.close();

    }


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
}
