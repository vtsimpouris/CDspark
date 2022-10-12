package _aux;

import org.apache.commons.math3.exception.DimensionMismatchException;

import java.util.*;
import java.util.stream.Stream;

public class lib {
    public static double[][] transpose(double[][] matrix) {
        double[][] t = new double[matrix[0].length][matrix.length];
        for (int i=0;i<matrix.length;i++)
            for (int j=0;j<matrix[0].length;j++)
                t[j][i]=matrix[i][j];
        return t;
    }

    public static double euclidean(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i]-in2[i];
            d+=(dd*dd);
        }
        return Math.sqrt(d);
    }

    public static double[] sum(double[] in1, double[] in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]+in2[i];
        return res;
    }

    public static double[] sub(double[] in1, double[] in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]-in2[i];
        return res;
    }

    //    Multiply by scalar
    public static double[] smul(double[] in1, double in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]*in2;
        return res;
    }

    public static double[] sadd(double[] in1, double in2) {
        double[] res = new double[in1.length];
        for (int i=0;i<in1.length;i++) res[i]=in1[i]+in2;
        return res;
    }

//    Get element-wise mean of list of vectors
    public static double[] elementwiseMean(List<double[]> in) {
        double[] res = new double[in.get(0).length];
//        Iterate over all vectors
        for (int i=0;i<in.size();i++) {
//            Add every element in vector to the result
            for (int j=0;j<in.get(0).length;j++) {
                res[j]+=in.get(i)[j];
            }
        }
//        Divide by number of vectors
        for (int j=0;j<in.get(0).length;j++) {
            res[j]/=in.size();
        }
        return res;
    }

    public static double l2(double[] in1) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i];
            d+=(dd*dd);
        }
        return Math.sqrt(d);
    }
    public static double l1(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = Math.abs(in1[i]-in2[i]);
            d+=dd;
        }
        return d;
    }

    public static double dot(double[] in1, double[] in2) {
        double d = 0;
        for (int i=0;i<in1.length;i++) {
            double dd = in1[i]*in2[i];
            d+=dd;
        }
        return d;
    }

    public static double[] mmul(double[] v, double[][] M) throws DimensionMismatchException {
        if (v.length != M[0].length){throw new DimensionMismatchException(v.length, M[0].length);}

        int m = M.length;
        double[] out = new double[m];

        for (int i = 0; i < m; i++) {
            out[i] = lib.dot(v,M[i]);
        }
        return out;
    }

    public static double[] znorm(double[] z) {
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double avg = sum/z.length;
        double var = sumSquare/z.length - avg*avg;
        var=Math.max(var + 1E-16, -var); // for floating point errors
        double stdev = Math.sqrt(var);
        for (int i=0;i<z.length;i++){
            z[i]=(z[i]-avg)/stdev;
            if(Double.isNaN(z[i])){
                System.out.println("debug: NaN result of znorm");
            }
        }
        return z;
    }

    //    zero-sum and l2-normalize a vector
    public static double[] l2norm(double[] v){
        double[] z = v.clone();
        double sum = Arrays.stream(z).reduce(0, Double::sum);
        double avg = sum / v.length;
        z = lib.sadd(v,-1*avg);
        double sumSquare = Arrays.stream(z).reduce(0, (a,b) -> a+b*b);
        double norm = Math.sqrt(sumSquare);
        z = lib.smul(z,1/norm);
        return z;
    }

    public static <T> Stream<T> getStream(Collection<T> collection, boolean parallel){
        if(parallel){
            return collection.parallelStream().parallel();
        }else{
            return collection.stream().sequential();
        }
    }

    public static double getDuration(long start, long stop){
        return (stop - start) / 1000d;
    }

}
