package data_reading;

import _aux.Pair;
import _aux.lib;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

public class DataReader {

    public static Pair<String[], double[][]> readColumnMajorCSV(String path, int maxN, int maxDim, boolean skipVar, int partition) {
        String delimiter = ",";
        int tmpN = skipVar ? maxN*2: maxN;

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));

//            Get Header
            String firstLine = br.readLine();
            String[] header = firstLine.split(delimiter);
            int n = header.length;
            int effN = Math.min(n, tmpN);

//            Parse data
            double[][] rows = new double[effN][maxDim];

//            Skip all non-partition rows
            for (int i = 0; i < maxDim*partition; i++) {
                br.readLine();
            }
            int m=0;
            while (br.ready() & m < maxDim) {
                String[] line = br.readLine().split(delimiter);
//                Distribute values over columns
                for (int i = 0; i < effN; i++) {
                    if (line[i].equals("nan")) {
                        System.out.println("nan value");
                    }

                    rows[i][m] = Double.parseDouble(line[i]);
                }
                m++;
            }

//            Remove the rows that have too low variance (if skipvar on)
            double[][] finalRows = new double[maxN][maxDim];
            if (skipVar) {
                int i=0;
                int j=0;
                while (i < maxN) {
                    double[] row = rows[j]; j++;
                    double[] rowCore = Arrays.copyOfRange(row, 1, row.length-1);
                    if (lib.var(rowCore) >= 1e-3) {
                        finalRows[i] = row; i++;
                    }
                }
            } else{finalRows = rows;}

            return new Pair<>(header, finalRows);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Pair<String[], double[][]> readRowMajorCSV(String path, int maxN, int maxDim, boolean skipVar, int partition) {
        String delimiter = ",";

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));

//            Discard first line
            br.readLine().split(delimiter);

//            Get Header
            String[] headers = new String[maxN];

//            Parse data
            ArrayList<double[]> rows = new ArrayList<>();
            int n = 0;
            while (br.ready() & n < maxN) {
                String[] line = br.readLine().split(delimiter);

                maxDim = Math.min(line.length - 1, maxDim);
                double[] row = IntStream.rangeClosed(partition*maxDim + 1,(partition+1)*maxDim).mapToDouble(i -> Double.parseDouble(line[i])).toArray();

//                Get the middle of a row to test for variance because we take the lagged variant of the row
                double[] rowCore = Arrays.copyOfRange(row, 1, row.length-1);

//                Skip rows if variance is too low
                if (!skipVar || lib.var(rowCore) >= 1e-3){
                    rows.add(row);
                    headers[n] = line[0];
                    n++;
                }
            }

//            Convert rows arraylist to array
            int effN = rows.size();

            double[][] res = rows.toArray( new double[effN][maxDim]);

            return new Pair<>(headers, res);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
