package algorithms.performance;

import _aux.*;
import algorithms.Algorithm;
import algorithms.StageRunner;
import bounding.RecursiveBounding;
import clustering.HierarchicalClustering;
import core.Parameters;
import bounding.SparkBounding;

import java.io.Serializable;

import java.util.*;
import java.util.concurrent.TimeUnit;


import org.apache.commons.lang3.time.StopWatch;

public class SimilarityDetective extends Algorithm implements Serializable {
    private static final long serialVersionUID = -2685444218382696361L;
    public transient HierarchicalClustering HC;
    public transient RecursiveBounding RB;
    public transient SparkBounding SB;

    public void print_results(Set<ResultTuple> results){
        Iterator iter = results.iterator();
        int i = 0;
        while (iter.hasNext() && i <10) {
            ResultTuple element = (ResultTuple) iter.next();
            //System.out.println(element);
            i++;
            }
        }
    public SimilarityDetective(Parameters par) {
        super(par);
        HC = new HierarchicalClustering(par);
    }

    @Override
    public Set<ResultTuple> run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);
//        Start the timer
        par.statBag.stopWatch.start();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

//        STAGE 1 - Compute pairwise distances if using empirical bounds
        //System.out.println(par.pairwiseDistances);
        par.setPairwiseDistances(
                stageRunner.run("Compute pairwise distances",
                        () -> lib.computePairwiseDistances(par.data, par.simMetric.distFunc, par.parallel), par.statBag.stopWatch)
        );

        /*par.setPairwiseDistances(
                stageRunner.run("Compute pairwise distances spark",
                        () -> lib.computePairwiseDistances_spark(par.data, par.simMetric.distFunc, par.parallel), par.statBag.stopWatch)
        );*/

//        STAGE 2 - Hierarchical clustering
        RB = new RecursiveBounding(par, HC.clusterTree);
        stageRunner.run("Hierarchical clustering", () -> HC.run(), par.statBag.stopWatch);

        StopWatch SBwatch = new StopWatch();
        SBwatch.start();
        SB = new SparkBounding(par, HC.clusterTree);
        stageRunner.run("SparkBounding", () -> SB.run(), par.statBag.stopWatch);
        SBwatch.stop();

        {
        par.java = true;
        par.spark = false;
        StopWatch RBwatch = new StopWatch();
        RBwatch.start();
        Set<ResultTuple> results = stageRunner.run("Recursive bounding local", () -> RB.run(), par.statBag.stopWatch);
        RBwatch.stop();
        System.out.println("Results: " + results.size());
        //results.clear();}

        /*RB = new RecursiveBounding(par, HC.clusterTree);
        {
        par.java = false;
        par.spark = true;
        if(par.statBag.stopWatch.isStopped()) {
            par.statBag.stopWatch.start();
        }
        Set<ResultTuple> spark_results = stageRunner.run("Recursive bounding spark", () -> RB.run(), par.statBag.stopWatch);
        System.out.println("Results: " + spark_results.size());
        print_results(spark_results);*/

        par.statBag.stopWatch.start();
        //par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        this.prepareStats();
            System.out.println("Total Spark Bounding execution time: "
                    + SBwatch.getTime(TimeUnit.MILLISECONDS) + " milli-seconds");
            System.out.println("Total Recursive Bounding execution time: "
                    + RBwatch.getTime(TimeUnit.MILLISECONDS) + " milli-seconds");
        return results;}
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
        par.statBag.otherStats.put("nCCs", par.statBag.nCCs.longValue());
        par.statBag.otherStats.put("avgCCSize", par.statBag.totalCCSize.get() / (double) par.statBag.nCCs.longValue());
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
