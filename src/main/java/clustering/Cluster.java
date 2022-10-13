package clustering;

import _aux.lib;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import similarities.DistanceFunction;

import java.util.*;

@RequiredArgsConstructor
public class Cluster {
    @Setter public int id;
    @NonNull private DistanceFunction dist;

    //    Points
    public ArrayList<Integer> tmpPoints = new ArrayList<>();
    public ArrayList<Integer> points;
    public HashMap<Integer, Double> distances;
    public boolean finalized = false;

//    Hypersphere statistics
    @Setter public Double radius;
    public double[] centroid;
    @NonNull public Integer centroidIdx;

    //    Relations
    @Setter public Cluster parent;
    public ArrayList<Cluster> children = new ArrayList<>();
    @Setter public int level;

//    Misc
    public Double score;

    public int size() {
        return points.size();
    }

    public int get(int i) {
        return points.get(i);
    }

    public void addPoint(int i){
        if (finalized) throw new RuntimeException("Cannot add points to a finalized cluster");
        tmpPoints.add(i);
    }

    public void addChild(Cluster sc){
        this.children.add(sc);
    }

    public ArrayList<double[]> getPoints(double[][] data){
        ArrayList<double[]> points = new ArrayList<>();
        ArrayList<Integer> pointsIdx = this.finalized ? this.points : this.tmpPoints;
        for (int i = 0; i < pointsIdx.size(); i++) {
            points.add(data[pointsIdx.get(i)]);
        }
        return points;
    }

//    Compute radius
    public void computeRadius(double[][] data) {
        double maxDist = 0;

//        Remove floating point error
        if (points.size() == 1){
            radius = 0.0;
            return;
        }

        for (int i = 0; i < points.size(); i++) {
            int pid = points.get(i);
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
        this.points = new ArrayList<>(tmpPoints);
        this.tmpPoints = null;
        this.distances = new HashMap<>(tmpPoints.size());

//        Initialize actual centroid
        computeGeometricCentroid(data);

//        Compute distances from centroid and determine radius
        computeRadius(data);


        tmpPoints = null;
        finalized = true;
    }

    public double getDistance(int pId, double[][] distMatrix){
//        If cluster is not final, compute distance from point centroid, otherwise get from local cache (geometric centroid)
        if (!finalized){
            return distMatrix[pId][centroidIdx];
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
