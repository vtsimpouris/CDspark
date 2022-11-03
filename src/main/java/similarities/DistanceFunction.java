package similarities;

import java.io.Serializable;

public interface  DistanceFunction extends Serializable {
    double dist(double[] x, double[] y);
}

