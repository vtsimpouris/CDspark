package algorithms.performance;

import _aux.*;
import algorithms.Algorithm;
import algorithms.StageRunner;
import bounding.RecursiveBounding;
import clustering.HierarchicalClustering;
import core.Parameters;

import java.io.Serializable;

import java.util.*;


import org.apache.commons.lang3.time.StopWatch;

public class SimilarityDetective extends Algorithm implements Serializable {
    private static final long serialVersionUID = -2685444218382696361L;
    public transient HierarchicalClustering HC;
    public transient RecursiveBounding RB;
    public transient RecursiveBounding RB_spark;
    Set<ResultTuple> results = null;
    Set<ResultTuple> java_results = null;


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

//        STAGE 2 - Hierarchical clustering
        RB = new RecursiveBounding(par, HC.clusterTree);
        stageRunner.run("Hierarchical clustering", () -> HC.run(), par.statBag.stopWatch);
        {   par.statBag.stopWatch.reset();
            par.statBag.stopWatch.start();
            RB = new RecursiveBounding(par, HC.clusterTree);
            RB.spark = true;
            results = stageRunner.run("Recursive bounding spark", () -> RB.run(), par.statBag.stopWatch);
            Iterator iter2 = results.iterator();
            update_results();
            while (iter2.hasNext()) {
                ResultTuple element = (ResultTuple) iter2.next();
                if (element.RHS.size() > 0 && par.n < 20) {
                    System.out.println(element);
                }
            }
            System.out.println("spark results: " + results.size());
        }
        //Set<ResultTuple> results = null;


        {
            RB.spark = false;
            par.statBag.stopWatch.reset();
            par.statBag.stopWatch.start();
            results = stageRunner.run("Recursive bounding", () -> RB.run(), par.statBag.stopWatch);
            Iterator iter = results.iterator();
            while (iter.hasNext()) {

            ResultTuple element = (ResultTuple) iter.next();
            if (element.RHS.size() > 0 && par.n < 20) {
                System.out.println(element);
            }
        }
        par.statBag.stopWatch.start();
        System.out.println("Java results: " + java_results.size());}





        //par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        this.prepareStats();

        return java_results;
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
    public void update_results(){
        if(RB.spark == true) {
            this.java_results = this.results;
        }

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
