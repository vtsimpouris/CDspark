package core;

import _aux.Pair;
import _aux.ResultTuple;
import _aux.lib;
import algorithms.Algorithm;
import algorithms.AlgorithmEnum;
import algorithms.baselines.SimpleBaseline;
import algorithms.baselines.SmartBaseline;
import algorithms.performance.SimilarityDetective;
import bounding.ApproximationStrategyEnum;
import clustering.ClusteringAlgorithmEnum;
import data_reading.DataReader;
import lombok.NonNull;
import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.shaded.com.google.common.base.Stopwatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.*;
import similarities.SimEnum;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        String codeVersion = "cd";

        Level logLevel;
        boolean saveStats;
        boolean saveResults;
        AlgorithmEnum algorithm;
        boolean parallel;
        boolean random;
        int seed;
        SimEnum simMetricName;
        String aggPattern;
        int maxPLeft;
        int maxPRight;
        boolean allowSideOverlap;
        String dataType;
        String inputPath;
        String outputPath;
        String[] headers;
        double[][] data;
        int n;
        int m;
        int partition;
        boolean empiricalBounding;
        double tau;
        double minJump;
        double shrinkFactor;
        int topK;
        ApproximationStrategyEnum approximationStrategy;

//        Read parameters from args
        if (args.length>0){
            int i=0;
            logLevel = Level.parse(args[i]); i++;
            algorithm = AlgorithmEnum.valueOf(args[i].toUpperCase()); i++;
            inputPath = args[i]; i++;
            outputPath = args[i]; i++;
            simMetricName = SimEnum.valueOf(args[i].toUpperCase()); i++;
            aggPattern = args[i]; i++;
            empiricalBounding = args[i].equals("true"); i++;
            dataType = args[i]; i++;
            n = Integer.parseInt(args[i]); i++;
            m = Integer.parseInt(args[i]); i++;
            partition = Integer.parseInt(args[i]); i++;
            tau = Double.parseDouble(args[i]); i++;
            minJump = Double.parseDouble(args[i]); i++;
            maxPLeft = Integer.parseInt(args[i]); i++;
            maxPRight = Integer.parseInt(args[i]); i++;
            allowSideOverlap = args[i].equals("true"); i++;
            shrinkFactor = Double.parseDouble(args[i]); i++;
            topK = Integer.parseInt(args[i]); i++;
            approximationStrategy = ApproximationStrategyEnum.valueOf(args[i].toUpperCase()); i++;
            seed = Integer.parseInt(args[i]); i++;
            parallel = args[i].equals("true"); i++;
            random = args[i].equals("true"); i++;
            saveStats = args[i].equals("true"); i++;
            saveResults = args[i].equals("true"); i++;
        } else {
            logLevel = Level.FINE;
            algorithm = AlgorithmEnum.SIMILARITY_DETECTIVE;
            inputPath = "/home/vtsimpouris/Desktop/";
            outputPath = "output";
            simMetricName = SimEnum.PEARSON_CORRELATION;
            aggPattern = "avg";
//            aggPattern = "custom(0.4-0.6)(0.5-0.5)";
            empiricalBounding = true;
            dataType = "stock";
            n = 100;
            m = (int) 500;
            partition = 0;
            tau = 0.94;
            minJump = 0.05;
            maxPLeft = 1;
            maxPRight = 3;
            allowSideOverlap = false;
            shrinkFactor = 0;
            topK = -1;
            approximationStrategy = ApproximationStrategyEnum.SIMPLE;
            seed = 0;
            parallel = false;
            random = false;
            saveStats = false;
            saveResults = false;
        }

//        Initiate logger
        Logger LOGGER = getLogger(logLevel);
        String resultPath = String.format("%s/results/%s_n%s_m%s_part%s_tau%.2f.csv", outputPath, dataType,
                n, m, partition, tau);
        String dateTime = (new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")).format(new Date());
        int threads = ForkJoinPool.getCommonPoolParallelism();
        int defaultDesiredClusters = 10; // set to Integer.MAX_VALUE for unrestricted and clustering based on epsilon only

        double epsilonMultiplier = 0.8;
        int maxLevels = 20;
        ClusteringAlgorithmEnum clusteringAlgorithm = ClusteringAlgorithmEnum.KMEANS;
        int breakFirstKLevelsToMoreClusters = 0;
        int clusteringRetries = 50;
        int nPriorityBuckets = 50;

        //        Get similarity function from enum
        MultivariateSimilarityFunction simMetric;
        switch (simMetricName){
            case PEARSON_CORRELATION: default: simMetric = new PearsonCorrelation(); break;
            case SPEARMAN_CORRELATION: simMetric = new SpearmanCorrelation(); break;
            case MULTIPOLE: simMetric = new Multipole(); break;
            case EUCLIDEAN_SIMILARITY: simMetric = new EuclideanSimilarity(); break;
            case MANHATTAN_SIMILARITY: simMetric = new MinkowskiSimilarity(1); break;
            case CHEBYSHEV_SIMILARITY: simMetric = new ChebyshevSimilarity(); break;
        }


//      ---  INPUT CORRECTIONS
//        Check if pleft and pright are correctly chosen
        if (!simMetric.isTwoSided() && maxPRight > 0){
            LOGGER.severe("The chosen similarity metric is not two-sided, but pright is > 0, adding pright to pleft");
            maxPLeft += maxPRight;
            maxPRight = 0;
        }

//        Check if empirical bounding is possible
        if (empiricalBounding && !simMetric.hasEmpiricalBounds()){
            LOGGER.severe("The chosen similarity metric does not support empirical bounding, setting empirical bounding to false");
            empiricalBounding = false;
        }

//        If custom aggregation pattern is chosen, check if minJump is set to 0
        if (aggPattern.startsWith("custom") && minJump > 0){
            LOGGER.severe("Custom aggregation pattern is chosen, but minJump is > 0, setting minJump to 0");
            minJump = 0;
        }

//        If topK query, reset tau and shrinkFactor
        if (topK > 0){
            LOGGER.severe("TopK query is chosen, setting tau to 0, minJump to 0");
            tau = 0.5;
            minJump = 0;
        } else {
            LOGGER.severe("Threshold query is chosen, setting shrinkFactor to 1");
            shrinkFactor = 1;
        }


        // Create aggregation function from pattern.
        // Is list because it also needs to consider subset correlations (i.e. mc(1,1), mc(1,2), mc(2,2))
        List<double[]> Wl = new ArrayList<>(maxPLeft);
        List<double[]> Wr = new ArrayList<>(maxPRight);
        switch (aggPattern){
            case "avg": {
                for (int i = 1; i < maxPLeft + 1; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d/i);
                    Wl.add(w);
                }
                for (int i = 1; i < maxPRight + 1; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d/i);
                    Wr.add(w);
                }
                break;
            }
            case "sum": {
                for (int i = 1; i < maxPLeft + 1; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d);
                    Wl.add(w);
                }
                for (int i = 1; i < maxPRight + 1; i++) {
                    double[] w = new double[i];
                    Arrays.fill(w, 1d);
                    Wr.add(w);
                }
                break;
            }
            default: {
                Pattern pattern = Pattern.compile("custom((\\(([0-9.]+-{0,1})+\\)){2})");
                Matcher matcher = pattern.matcher(aggPattern);
                if (matcher.matches()){
                    String[] leftRight = matcher.group(1).split("\\)\\(");
                    String[] left = leftRight[0].substring(1).split("-");
                    String[] right = leftRight[1].substring(0, leftRight[1].length()-1).split("-");
                    double[] fullLeft =Arrays.stream(left).mapToDouble(Double::parseDouble).toArray();
                    double[] fullRight =Arrays.stream(right).mapToDouble(Double::parseDouble).toArray();
                    for (int i = 1; i < maxPLeft + 1; i++) {
                        Wl.add(Arrays.copyOfRange(fullLeft, 0, i));
                    }
                    for (int i = 1; i < maxPRight + 1; i++) {
                        Wr.add(Arrays.copyOfRange(fullRight, 0, i));
                    }
                } else {
                    LOGGER.severe("Aggregation pattern not recognized, should be 'avg', 'sum' or 'custom(u0-u1-...-uPLeft)(w0-w1-...-wPRight)'");
                    System.exit(1);
                }
                break;
            }
        }

//        Set empirical bounding to false if metric does not have such bounds
        if (!simMetric.hasEmpiricalBounds()){
            empiricalBounding = false;
            LOGGER.info("Metric does not have empirical bounds, setting empiricalBounding to false");
        }

        double startEpsilon = simMetric.simToDist(0.81*simMetric.MAX_SIMILARITY);
        double maxApproximationSize = simMetric.simToDist(0.9*simMetric.MAX_SIMILARITY);

//        read data
        Pair<String[], double[][]> dataPair = getData(dataType, inputPath, n, m, partition, LOGGER);
        headers = dataPair.x;
        data = dataPair.y;

//        update parameters if we got less data
        n = data.length;
        m = data[0].length;

//        preprocess (if necessary)
        data = simMetric.preprocess(data);

        Parameters par = new Parameters(
                LOGGER,
                dateTime,
                codeVersion,
                saveStats,
                saveResults,
                resultPath,
                threads,
                algorithm,
                parallel,
                random,
                seed,
                simMetric,
                aggPattern,
                Wl,
                Wr,
                maxPLeft,
                maxPRight,
                allowSideOverlap,
                dataType,
                outputPath,
                headers,
                data,
                n,
                m,
                partition,
                empiricalBounding,
                tau,
                minJump,
                startEpsilon,
                epsilonMultiplier,
                maxLevels,
                defaultDesiredClusters,
                clusteringAlgorithm,
                breakFirstKLevelsToMoreClusters,
                clusteringRetries,
                shrinkFactor,
                maxApproximationSize,
                nPriorityBuckets,
                topK,
                approximationStrategy
        );
        par.init();

        run(par);
    }
    public static double[][] sparkComputePairwiseCorrelations(List<Double[]> list, JavaSparkContext sc,DistanceFunction df, int n){
        JavaRDD<Double[]> JavaRDD = sc.parallelize(list);

        JavaPairRDD<Double[], Double[]> cartesian = JavaRDD.cartesian(JavaRDD);
        List<Tuple2<Double[], Double[]>> temp = cartesian.collect();
        JavaDoubleRDD pairwise = cartesian.mapToDouble(s -> {
            double d = df.dist(ArrayUtils.toPrimitive(s._1), ArrayUtils.toPrimitive(s._2));
            //l.add(d);
            return Double.valueOf(d);
        });
        List<Double> returned = pairwise.collect();
        double[][] pairwiseDistances = new double[n][n];
        for (int i = 0; i < n; i++) {
            //System.out.println("i = " + i);
            for (int j = i + 1; j < n; j++) {
                pairwiseDistances[i][j] = returned.get(n * i + j);
                pairwiseDistances[j][i] = returned.get(n * i + j);
            }
        }
        return pairwiseDistances;
    }


    private static void run(@NonNull Parameters par) {
        par.LOGGER.info(String.format("----------- new run starting; querying %s with %s on %s part %d, n=%d ---------------------",
                par.simMetric, par.algorithm, par.dataType, par.partition, par.n));
        par.LOGGER.info("Starting time " + LocalDateTime.now());
        //sc.close();
        Algorithm algorithm;
        switch (par.algorithm){
            case SIMILARITY_DETECTIVE: default: algorithm = new SimilarityDetective(par); break;
            case SIMPLE_BASELINE: algorithm = new SimpleBaseline(par); break;
            case SMART_BASELINE: algorithm = new SmartBaseline(par); break;
        }

        SparkConf sparkConf = new SparkConf().setAppName("pairwise")
                .setMaster("local[16]").set("spark.executor.memory","32g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        DistanceFunction df = par.getSimMetric().distFunc;
        // prepare list of objects
        List<Double[]> list= new ArrayList<Double[]>();
        for (int i = 0; i < par.data.length; i++) {
            list.add(ArrayUtils.toObject(par.data[i]));
        }
        double[][] pairwiseDistances = new double[par.n][par.n];
        pairwiseDistances = sparkComputePairwiseCorrelations(list, sc, df, par.n);
        //System.out.println(Arrays.deepToString(pairwiseDistances));
        sc.close();

        Set<ResultTuple> results = algorithm.run();
        par.statBag.nResults = results.size();
        algorithm.printStats(par.statBag);

        par.LOGGER.info(String.format("Ending time " + LocalDateTime.now()));
        par.LOGGER.info("Number of reported results: " + results.size());

//        Save stats
        if (par.saveStats){
            par.statBag.saveStats(par);
        }

//        Save results
        if (par.saveResults){
            saveResults(new ArrayList<>(results), par);
        }

    }



    public static Pair<String[], double[][]> getData(String dataType, String inputPath, int n, int m, int partition, Logger LOGGER) {
        String dataPath;
        Pair<String[], double[][]> dataPair;

    //        ---------------------------- DATA READING ------------------------------------------

        LOGGER.info("--------------------- Loading data ---------------------");
        switch (dataType){
            case "weather_slp": {
                dataPath = String.format("%s/weather/1620_daily/slp_1620daily_filled_T.csv", inputPath);
                dataPair = DataReader.readRowMajorCSV(dataPath, n, m, true, partition);
            } break;
            case "weather_tmp": {
                dataPath = String.format("%s/weather/1620_daily/tmp_1620daily_filled_T.csv", inputPath);
                dataPair = DataReader.readRowMajorCSV(dataPath, n, m, true, partition);
            } break;
            case "fmri": {
                int[] n_steps = new int[]{237, 509, 1440, 3152, 9700};
                String[] dataPaths = new String[]{
                        String.format("%s/fmri/fmri_res8x10x8-237.csv", inputPath),
                        String.format("%s/fmri/fmri_res11x13x11-509.csv", inputPath),
                        String.format("%s/fmri/fmri_res16x19x16-1440.csv", inputPath),
                        String.format("%s/fmri/fmri_res22x26x22-3152.csv", inputPath),
                        String.format("%s/fmri/fmri_res32x38x32-9700.csv", inputPath),
                };

                dataPath = dataPaths[Math.min(n, 4)];

                n = n_steps[n];
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m, n < 9700, partition);

            } break;
            case "random": {
                dataPath = String.format("%s/random/random_n50000_m1000_seed0.csv", inputPath);
                dataPair = DataReader.readRowMajorCSV(dataPath, n, m, true, partition);
            } break;
            case "stock_log": {
                // path here needs correction
                dataPath = String.format("%s/stock/stock_0021daily/0021daily/stocks_0021daily_interpolated_full.csv", inputPath);
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m, true, partition);
            } break;
            case "stock":
            default: {
                dataPath = String.format("C:\\Users\\SKIKK\\Desktop\\stocks\\0021daily\\stocks_0021daily_interpolated_full.csv");
                dataPair = DataReader.readRowMajorCSV(dataPath, n, m, true, partition);
            } break;
        }

        return dataPair;
    }

    public static void saveResults(List<ResultTuple> results, Parameters parameters){
        try {
//            Make root dirs if necessary
            String rootdirname = Pattern.compile("\\/[a-z_0-9.]+.csv").matcher(parameters.resultPath).replaceAll("");
            new File(rootdirname).mkdirs();

            File file = new File(parameters.resultPath);

            FileWriter fw = new FileWriter(file, false);

//            Write header
            fw.write("lhs,rhs,headers1,headers2,sim\n");

            String[] headers = parameters.headers;

//            Write results
            for (int i = 0; i < results.size(); i++) {
                ResultTuple result = results.get(i);

                fw.write(String.format("%s,%s,%s,%s,%.4f%n",
                        result.LHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                        result.RHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                        String.join("-", result.lHeaders),
                        String.join("-", result.rHeaders),
                        result.similarity
                ));
            }

            fw.close();

        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static Logger getLogger(Level logLevel){
        Logger mainLogger = Logger.getLogger("com.logicbig");
        mainLogger.setUseParentHandlers(false);

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter() {
            private static final String format = "[%1$tF %1$tT] [%2$-7s] %3$s %n";
            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format,
                        new Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        lr.getMessage()
                );
            }
        });
        handler.setLevel(logLevel);
        mainLogger.addHandler(handler);
        mainLogger.setLevel(logLevel);

        return mainLogger;
    }
}