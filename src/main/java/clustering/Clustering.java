package clustering;

import similarities.DistanceFunction;

import java.util.ArrayList;
import java.util.List;

public class Clustering {

    public static ArrayList<Cluster> getKMeansMaxClusters(List<Integer> vIDs, double[][] data, double[][] distMatrix, double threshold,
                                                          int nClusters, DistanceFunction distFunc) {
        // allstocks -> stock dataset in array of size (n_dim rows, n_vec columns);
        // threshold -> epsilon -- max dist from centroid to point in cluster

        ArrayList<Cluster> allClusters = new ArrayList<>(Math.min(nClusters, 100));

//        Iterate over all vectors, and assign them to the closest cluster
        for (int i : vIDs) {
            Cluster minCluster=null;
            double minDist=Double.MAX_VALUE;

//            Find closest cluster
            for (Cluster c:allClusters) {
                double dist = c.getDistance(i,distMatrix);
                if (dist <= minDist) {
                    minDist = dist;
                    minCluster = c;
                }
            }
            if (minDist<threshold) { // no need to create a new
                minCluster.addPoint(i);
            } else if (allClusters.size()<nClusters) { // i can create a new cluster
                Cluster c = new Cluster(allClusters.size(), distFunc, i); // note that i is added to the listofcontents in the constructor
                allClusters.add(c);
            } else { //I cannot create a new cluster  -- just assign to the closest
                minCluster.addPoint(i);
            }
        }

//        Finalize clusters
        for (Cluster c:allClusters) {
            c.finalize(data);
        }

        return allClusters;
    }
}
