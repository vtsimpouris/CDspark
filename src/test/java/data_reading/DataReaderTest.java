package data_reading;

import _aux.Pair;
import core.Main;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;


public class DataReaderTest {
    private static Logger LOG = Main.getLogger(Level.INFO);
    private static DataReader dataReader;
    private static String inputPath = "/home/jens/tue/data";
    private static int n = 100;
    private static int m = 10;


    private void testDataSet(String dataType, int n, double[] targetVector0, double[] targetVector1, String targetHeader){
        //      ----------------------------------------- TESTS ---------------------------------------------

//        Test 1: Test if dataReader reads the right file and skips based on variance as expected
        Pair<String[], double[][]> dataPair = Main.getData(dataType, inputPath, n, m, 0, LOG);
        Assert.assertEquals(targetHeader, dataPair.x[0]);
        Assert.assertArrayEquals(targetVector0, dataPair.y[0], 1e-3);

//        Test 2: Test if dataReader reads right partition
        Pair<String[], double[][]> data = Main.getData(dataType, inputPath, n, m, 1, LOG);
        Assert.assertEquals(targetHeader, data.x[0]);
        Assert.assertArrayEquals(targetVector1, data.y[0], 1e-3);
    }

    @Test
    public void testStockData(){
        double[] targetVector0 = new double[]{51.9268, 51.9268, 51.9268, 50.2078, 49.2564, 50.9808, 54.662, 54.5395, 54.417, 54.2945};
        double[] targetVector1 = new double[]{54.4621, 55.1054, 54.7648, 54.4621, 54.4621, 54.4621, 54.4621, 52.57, 52.8404, 53.5485};
        String targetHeader = "32084.Nordamerika-USA-Dow-Jones-3M-Co._MMM";

        testDataSet("stock", n, targetVector0, targetVector1, targetHeader);
    }

    @Test
    public void testFMRIData(){
        double[] targetVector0 = new double[]{0.0070445207, 4.407682E-4, -0.0069068335, -0.0041662566, 0.008127926, -0.009505578, 0.023369465, -0.010516732, -0.013421742, -0.0033504171};
        double[] targetVector1 = new double[]{0.0021576253, -0.008002152, -0.0042262473, 0.009390015, -0.0012986916, -0.010124403, -0.0067973547, 0.008552558, -0.0033170758, -0.009353947};
        String targetHeader = "1_3_3";

        testDataSet("fmri", 0, targetVector0, targetVector1, targetHeader);

    }

    @Test
    public void testSLPData(){
        double[] targetVector0 = new double[]{10115.0, 10273.0, 10324.0, 10196.0, 10216.0, 10140.0, 10155.0, 10155.0, 10124.0, 10110.0};
        double[] targetVector1 = new double[]{10110.0, 10151.0, 10171.0, 10171.0, 10144.0, 10192.0, 10156.0, 10068.0, 10050.0, 10076.0};
        String targetHeader = "1026099999";

        testDataSet("weather_slp", n, targetVector0, targetVector1, targetHeader);
    }

    @Test
    public void testTMPData(){
        double[] targetVector0 = new double[]{-32.0, -42.0, -55.0, -99.0, -134.0, -151.0, -162.0, -170.0, -65.0, -47.0};
        double[] targetVector1 = new double[]{-79.0, -139.0, -158.0, -142.0, -113.0, -36.0, -39.0, -140.0, -165.0, -129.0};
        String targetHeader = "1026099999";

        testDataSet("weather_tmp", n, targetVector0, targetVector1, targetHeader);
    }

    @Test
    public void testRandomData(){
        double[] targetVector0 = new double[]{0.335, 0.2, 0.435, 0.53, 0.775, 0.278, 0.954, 0.451, 0.506, 0.067};
        double[] targetVector1 = new double[]{0.627, 0.836, 0.894, 0.523, 0.353, 0.254, 0.024, 0.818, 0.362, 0.213};
        String targetHeader = "0";

        testDataSet("random", n, targetVector0, targetVector1, targetHeader);
    }

    @Test
    public void testStockLogData(){
        double[] targetVector0 = new double[]{0.0028625973746332356, -9.532889187133797E-4, -0.004780123824719151, -0.0021585330362028365, -0.0021632023818249912, -0.0062756662640395255, 3.6313018616063175E-4, 3.629983704933615E-4, 3.6286665048868016E-4, 3.627350260440032E-4};
        double[] targetVector1 = new double[]{-0.0018264845260342888, 0.0, 0.0, 4.5693398099100335E-4, -0.002744740972919102, -4.58190156927496E-4, -4.5840019138732035E-4, -0.006439764665270076, -0.006946080254683906, -0.004657670273981007};
        String targetHeader = "# Amsterdam_AALB_NoExpiry";

        testDataSet("stock_log", n, targetVector0, targetVector1, targetHeader);
    }
}
