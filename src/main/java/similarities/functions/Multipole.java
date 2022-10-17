package similarities.functions;

import Jama.Matrix;
import _aux.lib;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Multipole extends MultivariateSimilarityFunction {

    public Multipole() {
//        Cluster based on angles
        this.distFunc = (double[] a, double[] b) -> Math.acos(Math.min(Math.max(lib.dot(a, b) / a.length, -1),1));
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return false;}
    @Override public double[][] preprocess(double[][] data) {
        return lib.znorm(data);
    }

    @Override public double sim(double[] x, double[] y) {
        return Math.min(Math.max(lib.dot(x, y) / x.length, -1),1);
    }

    @Override public double simToDist(double sim) {
        return Math.acos(sim);
    }
    @Override public double distToSim(double dist) {return Math.cos(dist);}

    @Override public ClusterBounds empiricalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr, true);
    }

    //    Theoretical bounds
    @Override public ClusterBounds theoreticalBounds(List<Cluster> LHS, List<Cluster> RHS, double[] Wl, double[] Wr) {
        return getBounds(LHS, RHS, null, Wl, Wr, false);
    }

    @Override public double[] theoreticalBounds(Cluster C1, Cluster C2){
        long ccID = getUniqueId(C1.id, C2.id);

        if (theoreticalPairwiseClusterCache.containsKey(ccID)) {
            return theoreticalPairwiseClusterCache.get(ccID);
        } else {
            double centroidDistance = this.distFunc.dist(C1.getCentroid(), C2.getCentroid());
            double lb = Math.cos(Math.min(Math.PI, centroidDistance + C1.getRadius() + C2.getRadius()));
            double ub = Math.cos(Math.max(0, centroidDistance - C1.getRadius() - C2.getRadius()));
            double[] bounds = new double[]{lb, ub};
            theoreticalPairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    public ClusterBounds getBounds(List<Cluster> LHS, List<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr, boolean empirical){
        double lower;
        double upper;

        if (RHS.size() > 0){
            throw new IllegalArgumentException("RHS must be empty for one-sided bounds");
        }

        double[][] lowerBoundsArray = new double[LHS.size()][LHS.size()];
        double[][] upperBoundsArray = new double[LHS.size()][LHS.size()];
        double highestAbsLowerBound = 0;

        // create upper and lower bound matrices U and L as described in paper
        for(int i=0; i< LHS.size(); i++) {
            // we can fill the diagonal with 1's since we always pick one vector from each cluster
            lowerBoundsArray[i][i] = 1;
            upperBoundsArray[i][i] = 1;
            Cluster c1 = LHS.get(i);
            for (int j = i + 1; j < LHS.size(); j++) {
                Cluster c2 = LHS.get(j);
                double[] bounds = empirical ? empiricalBounds(c1, c2, pairwiseDistances) : theoreticalBounds(c1, c2);

                if (bounds[0] > 0) {
                    highestAbsLowerBound = Math.max(highestAbsLowerBound, bounds[0]);
                } else if (bounds[1] < 0) {
                    highestAbsLowerBound = Math.max(highestAbsLowerBound, -bounds[1]);
                }

                lowerBoundsArray[i][j] = bounds[0];
                lowerBoundsArray[j][i] = bounds[0];

                upperBoundsArray[i][j] = bounds[1];
                upperBoundsArray[j][i] = bounds[1];
            }
        }

        // Calculate bounds on multipoles as described in Section: Application to Multipoles
        // Use jama for linear algebra. possible alternative: OjAlgo backend
        Matrix upperPairwise2 = new Matrix(upperBoundsArray);
        Matrix lowerPairwise2 = new Matrix(lowerBoundsArray);

        Matrix estimateMat2 = upperPairwise2.plus(lowerPairwise2).times(0.5);
        Matrix slackMat2 = upperPairwise2.minus(lowerPairwise2);

        double[] eigenVals2 = estimateMat2.eig().getRealEigenvalues();
        double slack2 = slackMat2.norm2();

        double smallestEig = 1;
        for(double e : eigenVals2){
            if(e < smallestEig){
                smallestEig = e;
            }
        }

        lower = 1 - (smallestEig + 0.5 * slack2);
        upper = 1 - (smallestEig - 0.5 * slack2);

        return new ClusterBounds(correctBound(lower), correctBound(upper), highestAbsLowerBound);
    }

}
