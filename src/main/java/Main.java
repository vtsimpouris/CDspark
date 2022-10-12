import _aux.Pair;
import _aux.lib;
import algorithms.AlgorithmEnum;
import data_reading.DataReader;
import lombok.NonNull;
import lombok.extern.java.Log;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.PearsonCorrelation;
import similarities.functions.SimEnum;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.*;

public class Main {
    public static void main(String[] args) {
        String codeVersion = "cd";

        Level logLevel;
        boolean saveStats;
        boolean saveResults;
        AlgorithmEnum algorithm;
        boolean parallel;
        boolean random;
        int run;
        SimEnum simMetricName;
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
            run = Integer.parseInt(args[i]); i++;
            parallel = args[i].equals("true"); i++;
            random = args[i].equals("true"); i++;
            saveStats = args[i].equals("true"); i++;
            saveResults = args[i].equals("true"); i++;
        } else {
            logLevel = Level.INFO;
            algorithm = AlgorithmEnum.CD;
            inputPath = "/home/jens/tue/data";
            outputPath = "output";
            simMetricName = SimEnum.PEARSON_CORRELATION;
            empiricalBounding = true;
            dataType = "stock";
            n = 1000;
            m = 1000;
            partition = 0;
            tau = 0.9;
            minJump = 0.05;
            maxPLeft = 1;
            maxPRight = 2;
            shrinkFactor = 1;
            k = -1;
            approximationStrategy = "simple";
            run = 0;
            parallel = false;
            random = false;
            saveStats = true;
            saveResults = false;
        }

//        Initiate logger
        Logger LOGGER = getLogger(logLevel);
        String resultPath = String.format("%s/results/%s_n%s_w%s_part%s_tau%.2f_umin%.3f.csv", outputPath, dataType,
                n, m, partition, tau);
        String dateTime = (new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")).format(new Date());
        int threads = ForkJoinPool.getCommonPoolParallelism();
        int defaultDesiredClusters = 10; // set to Integer.MAX_VALUE for unrestricted and clustering based on epsilon only
        double epsilonMultiplier = 0.8;
        int maxLevels = 20;
        boolean useKMeans = true;
        int breakFirstKLevelsToMoreClusters = 0;
        int clusteringRetries = 50;
        double maxApproximationSize = Math.sqrt(2 * m * (1- (0.5)));
        int nPriorityBuckets = 50;
        double startEpsilon = Math.sqrt(2*m*(1-0.8125));

        //        Get similarity function from enum
        MultivariateSimilarityFunction simMetric;
        switch (simMetricName){
            case PEARSON_CORRELATION: default: simMetric = new PearsonCorrelation(); break;
        }

//        read data
        Pair<String[], double[][]> dataPair = getData(dataType, inputPath, n, m, partition, LOGGER);
        headers = dataPair.x;
        data = dataPair.y;

//        preprocess (if necessary)
        data = simMetric.preprocess(data);
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

        double[][] data = dataPair.y;
        String[] headers = dataPair.x;



        return dataPair;
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