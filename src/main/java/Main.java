import _aux.Pair;
import _aux.Parameters;
import _aux.ResultTuple;
import algorithms.Algorithm;
import algorithms.AlgorithmEnum;
import algorithms.CorrelationDetective;
import clustering.ClusteringAlgorithmEnum;
import data_reading.DataReader;
import lombok.NonNull;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.PearsonCorrelation;
import similarities.SimEnum;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
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
        int k;
        String approximationStrategy;

//        Read parameters from args
        if (args.length>0){
            int i=0;
            logLevel = Level.parse(args[i]); i++;
            algorithm = AlgorithmEnum.valueOf(args[i]); i++;
            inputPath = args[i]; i++;
            outputPath = args[i]; i++;
            simMetricName = SimEnum.valueOf(args[i]); i++;
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
            shrinkFactor = Double.parseDouble(args[i]); i++;
            k = Integer.parseInt(args[i]); i++;
            approximationStrategy = args[i]; i++;
            seed = Integer.parseInt(args[i]); i++;
            parallel = args[i].equals("true"); i++;
            random = args[i].equals("true"); i++;
            saveStats = args[i].equals("true"); i++;
            saveResults = args[i].equals("true"); i++;
        } else {
            logLevel = Level.FINE;
            algorithm = AlgorithmEnum.CD;
            inputPath = "/home/jens/tue/data";
            outputPath = "output";
            simMetricName = SimEnum.PEARSON_CORRELATION;
            aggPattern = "sum";
//            aggPattern = "custom(0.4-0.6)(0.5-0.5)";
            empiricalBounding = true;
            dataType = "stock";
            n = 1000;
            m = (int) 1e7;
            partition = 0;
            tau = 0.9;
            minJump = 0.05;
            maxPLeft = 1;
            maxPRight = 2;
            shrinkFactor = 1;
            k = -1;
            approximationStrategy = "simple";
            seed = 0;
            parallel = true;
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
        double maxApproximationSize = Math.sqrt(2 * m * (1- (0.5)));
        int nPriorityBuckets = 50;

        //        Get similarity function from enum
        MultivariateSimilarityFunction simMetric;
        switch (simMetricName){
            case PEARSON_CORRELATION: default: simMetric = new PearsonCorrelation(); break;
        }


        // Create aggregation function from pattern
        double[] Wl = new double[maxPLeft];
        double[] Wr = new double[maxPRight];
        switch (aggPattern){
            case "avg": {
                Arrays.fill(Wl, 1.0/maxPLeft);
                Arrays.fill(Wr, 1.0/maxPRight);
                break;
            }
            case "sum": {
                Arrays.fill(Wl, 1.0);
                Arrays.fill(Wr, 1.0);
                break;
            }
            default: {
                Pattern pattern = Pattern.compile("custom((\\(([0-9.]+-{0,1})+\\)){2})");
                Matcher matcher = pattern.matcher(aggPattern);
                if (matcher.matches()){
                    String[] leftRight = matcher.group(1).split("\\)\\(");
                    String[] left = leftRight[0].substring(1).split("-");
                    String[] right = leftRight[1].substring(0, leftRight[1].length()-1).split("-");
                    Wl = Arrays.stream(left).mapToDouble(Double::parseDouble).toArray();
                    Wr = Arrays.stream(right).mapToDouble(Double::parseDouble).toArray();
                } else {
                    LOGGER.severe("Aggregation pattern not recognized, should be 'avg', 'sum' or 'custom(u0-u1-...-uPLeft)(w0-w1-...-wPRight)'");
                    System.exit(1);
                }

            }
        }

//        Set empirical bounding to false if metric does not have such bounds
        if (!simMetric.hasEmpiricalBounds()){
            empiricalBounding = false;
            LOGGER.info("Metric does not have empirical bounds, setting empiricalBounding to false");
        }

        //        TODO THIS DEPENDS ON THE DISTANCE FUNCTION!
        double startEpsilon = simMetric.simToDist(0.81*simMetric.MAX_SIMILARITY);

//        read data
        Pair<String[], double[][]> dataPair = getData(dataType, inputPath, n, m, partition, LOGGER);
        headers = dataPair.x;
        data = dataPair.y;

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
                k,
                approximationStrategy
        );
        par.init();

        run(par);
    }

    private static void run(@NonNull Parameters par) {
        par.LOGGER.info(String.format("----------- new run starting with %s on %s part %d, n=%d ---------------------",
                par.algorithm, par.dataType, par.partition, par.n));
        par.LOGGER.info("Starting time " + LocalDateTime.now());

        Algorithm algorithm;
        switch (par.algorithm){
            case CD: default: algorithm = new CorrelationDetective(par); break;
        }
        List<ResultTuple> results = algorithm.run();

        par.LOGGER.info(String.format("Ending time " + LocalDateTime.now()));
        par.LOGGER.info("Number of reported results: " + results.size());

//        Save stats
        if (par.saveStats){
            par.statBag.saveStats(par);
        }

//        Save results
        if (par.saveResults){
            saveResults(results, par);
        }

    }

    private static Pair<String[], double[][]> getData(String dataType, String inputPath, int n, int m, int partition, Logger LOGGER) {
        String dataPath;
        Pair<String[], double[][]> dataPair;

    //        ---------------------------- DATA READING ------------------------------------------

        LOGGER.info("--------------------- Loading data ---------------------");
        switch (dataType){
            case "weather_slp": {
                dataPath = String.format("%s/weather/thesis/slp_base_3m.csv", inputPath);
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m+1, true, partition);
            } break;
            case "weather_tmp": {
                dataPath = String.format("%s/weather/thesis/tmp_base_3m.csv", inputPath);
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m+1, true, partition);
            } break;
            case "crypto": {
                dataPath = String.format("%s/crypto/thesis/base.csv", inputPath);
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m+1, true, partition);
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

                dataPath = dataPaths[Math.min(n+1, 4)];

                n = n_steps[n];
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m+1, n < 9700, partition);

            } break;
            case "random": {
                dataPath = String.format("%s/random/random_n50000_m1000_seed0.csv", inputPath);
                dataPair = DataReader.readRowMajorCSV(dataPath, n, m+1, true, partition);
            } break;
            case "stock_log": {
                dataPath = String.format("%s/stock/stocks_2020_04_10min_logreturn_full.csv", inputPath);
                dataPair = DataReader.readColumnMajorCSV(dataPath, n, m+1, true, partition);
            } break;
            case "stock":
            default: {
                dataPath = String.format("%s/stock/1620daily/stocks_1620daily_interpolated_nogaps.csv", inputPath);
                dataPair = DataReader.readRowMajorCSV(dataPath, n, m+1, true, partition);
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

    private static Logger getLogger(Level logLevel){
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