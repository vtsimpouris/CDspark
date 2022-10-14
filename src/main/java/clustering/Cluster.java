package clustering;

import _aux.lib;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import similarities.DistanceFunction;

import java.util.*;

public class Cluster {
    @Setter public int id;
    private DistanceFunction dist;

    //    Points
    public ArrayList<Integer> tmpPointsIdx = new ArrayList<>();
    public ArrayList<Integer> pointsIdx;
    @Getter HashMap<Integer, Double> distances;
    public boolean finalized = false;

//    Hypersphere statistics
    @Setter @Getter public Double radius;
    public double[] centroid;
    public Integer centroidIdx;

    //    Relations
    @Setter @Getter public Cluster parent;
    @Getter public ArrayList<Cluster> children = new ArrayList<>();
    @Setter @Getter public int level;

//    Misc
    public Double score;

    public Cluster(DistanceFunction dist, int centroidIdx) {
        this.centroidIdx = centroidIdx;
        this.dist = dist;
        this.tmpPointsIdx = new ArrayList<>(Collections.singletonList(centroidIdx));
    }

    public int size() {
        return pointsIdx.size();
    }

    public int get(int i) {
        return pointsIdx.get(i);
    }

    public void addPoint(int i){
        if (finalized) throw new RuntimeException("Cannot add points to a finalized cluster");
        tmpPointsIdx.add(i);
    }

    public void addChild(Cluster sc){
        this.children.add(sc);
    }

    public ArrayList<double[]> getPoints(double[][] data){
        ArrayList<double[]> points = new ArrayList<>();
        ArrayList<Integer> pointsIdx = this.finalized ? this.pointsIdx : this.tmpPointsIdx;
        for (int i = 0; i < pointsIdx.size(); i++) {
            points.add(data[pointsIdx.get(i)]);
        }
        return points;
    }

//    Compute radius
    public void computeRadius(double[][] data) {
        double maxDist = 0;

//        Remove floating point error
        if (pointsIdx.size() == 1){
            radius = 0.0;
            return;
        }

        for (int i = 0; i < pointsIdx.size(); i++) {
            int pid = pointsIdx.get(i);
            double dist;
            if (!distances.containsKey(pid)) {
                dist = this.dist.dist(centroid, data[this.get(i)]);
                distances.put(pid, dist);
            } else {
                dist = distances.get(pid);
            }
            maxDist = Math.max(maxDist, dist);
        }

        radius = maxDist;
    }

//    Compute centroid
    public void computeGeometricCentroid(double[][] data) {
        centroid = lib.elementwiseAvg(getPoints(data));
        centroidIdx = null;
    }

    public void finalize(double[][] data){
        if (finalized) throw new RuntimeException("Cluster already finalized");

//        Create final content array
        this.pointsIdx = new ArrayList<>(tmpPointsIdx);
        this.distances = new HashMap<>(tmpPointsIdx.size());

//        Initialize actual centroid
        computeGeometricCentroid(data);

//        Compute distances from centroid and determine radius
        computeRadius(data);


        finalized = true;
    }

    public double getDistance(int pId, double[][] pairwiseDistances){
//        If cluster is not final, compute distance from point centroid, otherwise get from local cache (geometric centroid)
        if (!finalized){
            return pairwiseDistances[pId][centroidIdx];
        } else {
            return distances.get(pId);
        }
    }

    public Double getScore(){
        if (score==null) {
            score = this.radius / this.size();
        }
        return score;
    }
}
