package clustering;

import _aux.lib;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import similarities.DistanceFunction;

import java.util.ArrayList;

@RequiredArgsConstructor
public class Cluster {
    @NonNull public int id;
    public ArrayList<double[]> points = new ArrayList<>();
    public Double radius;
    public double[] centroid;
    @Setter public Cluster parent;
    public ArrayList<Cluster> children;

    @NonNull private DistanceFunction dist;

//    Compute radius if not computed yet
    public double getRadius() {
        if (radius == null) {
            radius = computeRadius();
        }
        return radius;
    }

//    Compute radius
    public double computeRadius() {
        double maxDist = 0;
        for (int i = 0; i < points.size(); i++) {
            double dist = this.dist.dist(centroid, points.get(i));
                if (dist > maxDist) {
                    maxDist = dist;
            }
        }
        return maxDist;
    }

//    Compute centroid if not computed yet
    public double[] getCentroid() {
        if (centroid == null) {
            centroid = computeCentroid();
        }
        return centroid;
    }

//    Compute centroid
    public double[] computeCentroid() {
        return lib.elementwiseMean(points);
    }

}
