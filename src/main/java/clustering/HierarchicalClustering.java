package clustering;

import _aux.Parameters;

import java.util.ArrayList;
import java.util.Collections;

public class HierarchicalClustering {
    private Parameters par;
    public int globalClusterID = 0;
    public ArrayList<ArrayList<Cluster>> clusterTree;

    public HierarchicalClustering(Parameters par){
        this.par = par;
        this.clusterTree = new ArrayList<>(par.maxLevels + 1);
        for (int i = 0; i <= par.maxLevels + 1; i++) {
            this.clusterTree.add(new ArrayList<>());
        }
    }
    public void run(){
//        Create root cluster
        Cluster root = new Cluster(par.simMetric.distFunc, 0);
        root.setId(globalClusterID++);
        root.setLevel(0);
        for (int i = 1; i < par.n; i++) {
            root.addPoint(i);
        }

//        Finalize root and add to cluster tree and allClusters
        root.finalize(par.data);
        clusterTree.get(0).add(root);

//        Create clustering tree
        recursiveClustering(root, par.startEpsilon);

//        Set total clusters for simmetric
        par.simMetric.setTotalClusters(globalClusterID);
    }

    public void recursiveClustering(Cluster c, double distThreshold){
        ArrayList<Cluster> subClusters = makeAndGetSubClusters(c, distThreshold);

        double nextThreshold = 0d;

        for (Cluster sc : subClusters) {
        // If under maxlevel, keep multiplying epsilon, otherwise change threshold such that we only get singletons
            if (sc.level < par.maxLevels - 1) {
                nextThreshold = sc.getRadius() * par.epsilonMultiplier;
            }
            if (sc.level < par.maxLevels && sc.size() > 1) {
                recursiveClustering(sc,nextThreshold);
            }
        }
    }

    public ArrayList<Cluster> makeAndGetSubClusters(Cluster c, double epsilon){
        ArrayList<Cluster> subClusters;
        ArrayList<Cluster> bestSubClusters = null;
        double bestDistance = Double.MAX_VALUE;

        for (int i = 0; i < par.clusteringRetries; i++) {
            Collections.shuffle(c.pointsIdx, par.randomGenerator);

//            Variable cluster parameters
            int nDesiredClusters = par.defaultDesiredClusters;
            if (epsilon <= 0 || c.level == par.maxLevels) nDesiredClusters = c.size();
            if (c.level < par.breakFirstKLevelsToMoreClusters) nDesiredClusters *= 5;

            switch (par.clusteringAlgorithm) {
                default:
                case KMEANS:
                    subClusters = Clustering.getKMeansMaxClusters(c.pointsIdx, par.data, par.pairwiseDistances,
                            epsilon, nDesiredClusters, par.simMetric.distFunc);
                    break;
            }

            double totalScore = subClusters.stream().mapToDouble(Cluster::getScore).sum();
            if (totalScore < bestDistance) {
                bestDistance = totalScore;
                bestSubClusters = (ArrayList<Cluster>) subClusters.clone();
            }
        }
        subClusters = bestSubClusters;

//        Set parent-child relationships
        for (Cluster sc : subClusters) {
            sc.setParent(c);
            c.addChild(sc);

//            Update tree statistics
            sc.setId(globalClusterID++);
            sc.setLevel(c.level + 1);

            this.clusterTree.get(sc.level).add(sc);
        }

        return subClusters;
    }
}
