package algorithms.performance;

import _aux.*;
import algorithms.Algorithm;
import algorithms.StageRunner;
import bounding.RecursiveBounding;
import bounding.RecursiveBounding_spark;
import clustering.Cluster;
import clustering.HierarchicalClustering;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import core.Parameters;
import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.shaded.com.google.common.base.Stopwatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.collect.ImmutableList;
import similarities.DistanceFunction;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.commons.lang3.time.StopWatch;

public class SimilarityDetective extends Algorithm implements Serializable {
    private static final long serialVersionUID = -2685444218382696361L;
    public transient HierarchicalClustering HC;
    public transient RecursiveBounding RB;
    public transient RecursiveBounding_spark RBs;


    public SimilarityDetective(Parameters par) {
        super(par);
        HC = new HierarchicalClustering(par);
    }

    @Override
    public Set<ResultTuple> run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);
//        Start the timer
        par.statBag.stopWatch.start();

//        STAGE 1 - Compute pairwise distances if using empirical bounds
        //System.out.println(par.pairwiseDistances);
        par.setPairwiseDistances(
                stageRunner.run("Compute pairwise distances",
                        () -> lib.computePairwiseDistances(par.data, par.simMetric.distFunc, par.parallel), par.statBag.stopWatch)
        );

//        STAGE 2 - Hierarchical clustering
        RB = new RecursiveBounding(par, HC.clusterTree);
        stageRunner.run("Hierarchical clustering", () -> HC.run(), par.statBag.stopWatch);
        //RBs = new RecursiveBounding_spark(par, HC.clusterTree);
        //System.out.println(Arrays.deepToString(par.pairwiseDistances));

//        STAGE 3 - Recursive bounding
        //SparkConf sparkConf = new SparkConf().setAppName("RB")
        //        .setMaster("local[8]").set("spark.executor.memory","4g");
        // start a spark context
        //JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //List<RecursiveBounding> list= new ArrayList<RecursiveBounding>();
        //list.add(RB);

        /*StopWatch stopWatch = new StopWatch();
        JavaRDD<RecursiveBounding> JavaRDD = sc.parallelize(list);
        JavaRDD<Set<ResultTuple>> returnedRDD;
        stopWatch.start();
        returnedRDD = JavaRDD.map(s -> {
            s.run();
            return s.results;
        });

        List<Set<ResultTuple>> returned = returnedRDD.collect();
        System.out.println(returned.get(0));
        stopWatch.stop();*/
        // Print out the total time of the watch
        //System.out.println("Spark RB Time: " + stopWatch.getTime());

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Set<ResultTuple> results = stageRunner.run("Recursive bounding", () -> RB.run(), par.statBag.stopWatch);
        System.out.println(results);
        stopWatch.stop();
        //System.out.println(RB.clusterTree);
        // Print out the total time of the watch
        System.out.println("Java RB Time: " + stopWatch.getTime());



        //par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        this.prepareStats();

        return results;
    }



    @Override
    public void prepareStats(){
//        Manually set postprocessing stage time
        double postProcessTime = (double) par.statBag.otherStats.get("postProcessTime");
        par.statBag.stageDurations.add(new Stage("Post-processing (during Recursive Bounding)", postProcessTime));

//        Subtract postprocessing time from bounding time
        Stage boundingStage = par.statBag.stageDurations.get(2);
        boundingStage.setDuration(boundingStage.getDuration() - postProcessTime);

        par.statBag.otherStats.put("nLookups", par.simMetric.nLookups.get());
        par.statBag.otherStats.put("nCCs", par.statBag.nCCs.get());
        par.statBag.otherStats.put("avgCCSize", par.statBag.totalCCSize.get() / (double) par.statBag.nCCs.get());
    }

    @Override
    public void printStats(StatBag statBag) {
        par.LOGGER.fine("----------- Run statistics --------------");

//        CCs and lookups
        par.LOGGER.fine(String.format("%-30s %d","nLookups:", (Long) par.statBag.otherStats.get("nLookups")));
        par.LOGGER.fine(String.format("%-30s %d","nCCs:", (Long) par.statBag.otherStats.get("nCCs")));
        par.LOGGER.fine(String.format("%-30s %.1f","avgCCSize:", (double) par.statBag.otherStats.get("avgCCSize")));

//        DCCs
        par.LOGGER.fine(String.format("%-30s %d","nPosDCCs:", (Integer) par.statBag.otherStats.get("nPosDCCs")));
        par.LOGGER.fine(String.format("%-30s %d","nNegDCCs:", (Long) par.statBag.otherStats.get("nNegDCCs")));

        this.printStageDurations(statBag);
    }

}
