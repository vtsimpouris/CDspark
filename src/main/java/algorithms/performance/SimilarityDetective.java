package algorithms.performance;

import _aux.*;
import algorithms.Algorithm;
import algorithms.StageRunner;
import bounding.RecursiveBounding;
import clustering.HierarchicalClustering;
import com.google.common.primitives.Doubles;
import core.Parameters;
import org.apache.commons.lang.ArrayUtils;
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
import similarities.DistanceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
        JavaPairRDD<Double,Integer> result = null;
        for (int i = 0; i < d.length; i++) {
            List<Tuple2> data =  Arrays.asList(new Tuple2(Doubles.asList(d[i]), i));
            JavaRDD rdd = sc.parallelize(data);
            result = JavaPairRDD.fromJavaRDD(rdd);
            System.out.println(result.collect());
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
        System.out.println("babis");
//        Start the timer
        par.statBag.stopWatch.start();

//        STAGE 1 - Compute pairwise distances if using empirical bounds
        //System.out.println(par.pairwiseDistances);
        par.setPairwiseDistances(
                stageRunner.run("Compute pairwise distances",
                        () -> lib.computePairwiseDistances(par.data, par.simMetric.distFunc, par.parallel), par.statBag.stopWatch)
        );
        System.out.println("babis");
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf().setAppName("spark_test");
        sparkConf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaPairRDD<Double,Integer> result = createRDDfromData(par.data,sc);
        System.out.println(result.collect());



        //List<Double> list = Doubles.asList(all_pairs);
        //System.out.println(result.collect());
        //JavaRDD<Double> pairRDD = sc.parallelize(list);
        //System.out.println(pairRDD.collect());
        //DistanceFunction distFunc = par.simMetric.distFunc;
        //JavaPairRDD<Double, Double> cartesian = pairRDD.cartesian(pairRDD);
        // JavaRDD<Tuple2<InputType0, InputType1>> crossProduct = cartesian
        //      .map(scalaTuple -> new Tuple2<>(scalaTuple._1, scalaTuple._2));
        //System.out.println(cartesian.collect());
        /*JavaPairRDD<Double,Double> convert_D_to_d =  cartesian.mapToPair(
                (Tuple2<Double,Double> pair) ->  new Tuple2<Double,Double>(
                        (pair._1()), new Double(distFunc.dist(new double[]{pair._1()},new double[]{pair._2()}))));*/
        // need new distance metrics to abide to spark (Double)
        //System.out.println(distFunc.dist(new double[]{1.0,2.0},new double[]{1.0,3.0}));


        /*JavaRDD<String> words = input.flatMap(
new FlatMapFunction<String, String>() {
public Iterable<String> call(String x) {
return Arrays.asList(x.split(" "));
}}); */



        //System.out.println(par.n);
        //System.out.println(par.m);
        //System.out.println(par.data[0][0]);
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
