package clustering;

import _aux.lib;
import core.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HierarchicalClustering {
    private Parameters par;
    public int globalClusterID = 0;
    public ArrayList<ArrayList<Cluster>> clusterTree;

    public HierarchicalClustering(Parameters par){
        this.par = par;
        this.clusterTree = new ArrayList<>(par.maxLevels);
        for (int i = 0; i < par.maxLevels; i++) {
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
            if (sc.level < par.maxLevels - 2) {
                nextThreshold = sc.getRadius() * par.epsilonMultiplier;
            }
            if (sc.level < par.maxLevels - 1 && sc.size() > 1) {
                recursiveClustering(sc,nextThreshold);
            }
        }
    }

    public ArrayList<Cluster> makeAndGetSubClusters(Cluster c, double epsilon){
        ArrayList<Cluster>[] subClustersPerTry = new ArrayList[par.clusteringRetries];

//        Try different clustering runs in parallel, return score for each run
        List<Double> clusteringScores = lib.getStream(IntStream.range(0, par.clusteringRetries).boxed(), par.parallel).unordered()
                .map(i -> {
                     List<Integer> points = new ArrayList<>(c.pointsIdx);
                    Collections.shuffle(points, par.randomGenerator);

                    //  Variable cluster parameters
                    int nDesiredClusters = par.defaultDesiredClusters;
                    if (epsilon <= 0 || c.level == par.maxLevels) nDesiredClusters = c.size();
                    if (c.level < par.breakFirstKLevelsToMoreClusters) nDesiredClusters *= 5;

                    List<Cluster> localSubClusters;
                    switch (par.clusteringAlgorithm) {
                        default:
                        case KMEANS:
                            localSubClusters = Clustering.getKMeansMaxClusters(points, par.data, par.pairwiseDistances,
                                    epsilon, nDesiredClusters, par.simMetric.distFunc);
                            break;
                    }
                    subClustersPerTry[i] = new ArrayList<>(localSubClusters);
                    return localSubClusters.stream().mapToDouble(Cluster::getScore).sum();
                }).collect(Collectors.toList());

//        Get clustering with best score
        double bestScore = clusteringScores.get(0);
        ArrayList<Cluster> bestSubClusters = subClustersPerTry[0];

        for (int i = 1; i < par.clusteringRetries; i++) {
            if (clusteringScores.get(i) < bestScore) {
                bestScore = clusteringScores.get(i);
                bestSubClusters = subClustersPerTry[i];
            }
        }


//        Set parent-child relationships
        for (Cluster sc : bestSubClusters) {
            sc.setParent(c);
            c.addChild(sc);

//            Update tree statistics
            sc.setId(globalClusterID++);
            sc.setLevel(c.level + 1);

            this.clusterTree.get(sc.level).add(sc);
        }

        return bestSubClusters;
    }

    public Cluster[] getAllClusters(){
        Cluster[] allClusters = new Cluster[this.globalClusterID];
//        Iterate over all cluster levels and add to list (considering cid as index in list)

        for (List<Cluster> level : this.clusterTree){
            level.forEach(c -> allClusters[c.id] = c);
        }
        return allClusters;
    }
}
