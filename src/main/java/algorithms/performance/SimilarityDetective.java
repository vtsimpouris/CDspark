package algorithms.performance;

import _aux.*;
import algorithms.Algorithm;
import algorithms.StageRunner;
import bounding.RecursiveBounding;
import clustering.HierarchicalClustering;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import core.Parameters;
import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.shaded.com.google.common.base.Stopwatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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

public class SimilarityDetective extends Algorithm {
    public HierarchicalClustering HC;
    public RecursiveBounding RB;

    public Double[] convert_d_to_D(double[] array){
        Double[] temp = new Double[array.length];
        for (int i = 0; i < array.length; i++){
            temp[i] = Double.valueOf(array[i]);

        }
        return temp;

    };

    public JavaPairRDD<Double,Integer> createRDDfromData(double[][] d, JavaSparkContext sc){
        JavaPairRDD temp = null;
        JavaPairRDD<Double,Integer> result = null;
        /*for (int i = 0; i < d.length; i++){
            Double[] t = new Double[d[i].length];
            for (int j = 0; j < d[i].length; j++) {
                t[j] = d[i][j];
                //System.out.println(t[j]);
            }
            List<Double[]> t2 = new ArrayList<Double[]>();
            t2.add(t);
            JavaRDD rdd = sc.parallelize(t2);

            /* TODO: ADD key
            List<Tuple2> data =  Arrays.asList(new Tuple2(i, t2));
            System.out.println(data.get(0)._2.getClass());
            //Arrays.deepToString(list.toArray())
            JavaRDD rdd = sc.parallelize(t2);
            //result = JavaPairRDD.fromJavaRDD(rdd);

            // code for printing array from RDD
            //Double[] t3 = (Double[]) (rdd.collect().toArray()[0]);
            //System.out.println(Arrays.toString(t3));
            //System.out.println(result.collect());

        }*/
        for (int i = 0; i < d.length; i++) {
            List<Tuple2> data =  Arrays.asList(new Tuple2(Doubles.asList(d[i]), i));
            JavaRDD rdd = sc.parallelize(data);
            temp= JavaPairRDD.fromJavaRDD(rdd);
            if(result == null){
                result = temp;
            }else {
                result = result.union(temp);
            }
        }
        return result;
    };

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
        System.out.println("his: \n ");
        System.out.println(Arrays.deepToString(par.pairwiseDistances));

//        STAGE 2 - Hierarchical clustering
        RB = new RecursiveBounding(par, HC.clusterTree);
        stageRunner.run("Hierarchical clustering", () -> HC.run(), par.statBag.stopWatch);

//        STAGE 3 - Recursive bounding
        Set<ResultTuple> results = stageRunner.run("Recursive bounding", () -> RB.run(), par.statBag.stopWatch);

        par.statBag.stopWatch.stop();
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
