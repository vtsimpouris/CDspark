import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.EuclideanSimilarity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Sandbox {
    public static void main(String[] args) {
        double[] a = new double[]{1, 2, 3};
        double[] z = a.clone();
        a[0] = 0;
        z[1] = 0;

        System.out.println(Arrays.toString(a));
        System.out.println(Arrays.toString(z));

    }

//    public static void main(String[] args) {
//        Random r = new Random(1);
//
////        Check if euclidean similarity bounds are correct
//        int n =100;
//        int m = 1000;
//        double[][] data = new double[n][m];
//
////        Generate random data
//        for (int i = 0; i < n; i++) {
//            for (int j = 0; j < m; j++) {
//                data[i][j] = r.nextDouble();
//            }
//        }
////        L2norm
//        data = lib.l2norm(data);
//
//        MultivariateSimilarityFunction simFunc = new EuclideanSimilarity();
//
////        Make singleton clusters
//        Cluster C1 = new Cluster(simFunc.distFunc, 0); C1.finalize(data);
//        Cluster C3 = new Cluster(simFunc.distFunc, 1); C3.finalize(data);
//
//        List<Cluster> LHS = new ArrayList<>(Arrays.asList(C1));
//        List<Cluster> RHS = new ArrayList<>(Arrays.asList(C1,C3));
//
////        List<Cluster> clusters = getClusters(data, simFunc.distFunc);
////        List<Cluster> LHS = clusters.subList(0, 1);
////        List<Cluster> RHS = clusters.subList(1, 3);
//        double[] Wl = new double[]{1};
//        double[] Wr = new double[]{1, 1};
//
////        Get actual distance bounds
//        ArrayList<Double> sims = new ArrayList<>();
//
////        Iterate over all point combinations
//        for (double[] x : LHS.get(0).getPoints(data)) {
//            for (double[] y : RHS.get(0).getPoints(data)) {
//                for (double[] z : RHS.get(1).getPoints(data)) {
//                    sims.add(1 / (1 + lib.euclidean(x, lib.add(y, z))));
//                }
//            }
//        }
//        double actLB = sims.stream().mapToDouble(Double::doubleValue).min().getAsDouble();
//        double actUB = sims.stream().mapToDouble(Double::doubleValue).max().getAsDouble();
//
////        Get theoretical distance bounds
//        ClusterBounds bounds = simFunc.theoreticalSimilarityBounds(LHS, RHS, Wl, Wr);
//
//
//        System.out.println(LHS.size());
//    }
//
//    public static List<Cluster> getClusters(double[][] data, DistanceFunction distFunc){
////        Create clusters
//        Cluster C1 = new Cluster(distFunc, 0);
//        for (int i = 1; i < 10; i++) {
//            C1.addPoint(i);
//        }
//        C1.finalize(data);
//
//        Cluster C2 = new Cluster(distFunc, 10);
//        for (int i = 11; i < 20; i++) {
//            C2.addPoint(i);
//        }
//        C2.finalize(data);
//
//        Cluster C3 = new Cluster(distFunc, 20);
//        for (int i = 21; i < 30; i++) {
//            C3.addPoint(i);
//        }
//        C3.finalize(data);
//
//        return new ArrayList<>(Arrays.asList(C1, C2, C3));
//    }
}
