package bounding;

import _aux.Parameters;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import similarities.MultivariateSimilarityFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@RequiredArgsConstructor
public class ClusterCombination {
    @NonNull ArrayList<Cluster> LHS;
    @NonNull ArrayList<Cluster> RHS;

    @Setter @Getter boolean isPositive = false;
    @Setter @Getter boolean isDecisive = false;

    @Getter double LB = -Double.MAX_VALUE;
    @Getter double UB = Double.MAX_VALUE;
    @Getter double maxLowerBoundSubset = -Double.MAX_VALUE;

//    ------------------- METHODS -------------------

    public ArrayList<Cluster> getClusters(){
        ArrayList<Cluster> out = new ArrayList<>();
        out.addAll(LHS); out.addAll(RHS);
        return out;
    }

    public void swapLeftRightSide(){
        ArrayList<Cluster> left = this.LHS;
        this.LHS = this.RHS;
        this.RHS = left;
    }

    public void checkAndSetLB(double LB){
        this.LB = Math.max(LB, this.LB);
    }

    public void checkAndSetUB(double UB){
        this.UB = Math.min(UB, this.UB);
    }

    public void checkAndSetMaxSubsetLowerBoundSubset(double lowerBoundSubset){
        this.maxLowerBoundSubset = Math.max(this.maxLowerBoundSubset, lowerBoundSubset);
    }

    public void bound(MultivariateSimilarityFunction simMetric, boolean empiricalBounding, double[] Wl, double[] Wr, double[][] pairwiseDistances){
        ClusterBounds bounds;

        if (empiricalBounding){
            bounds = simMetric.empiricalBounds(LHS, RHS, pairwiseDistances, Wl, Wr);
        } else {
            bounds = simMetric.theoreticalBounds(LHS, RHS, Wl, Wr);
        }
        this.checkAndSetLB(bounds.getLB());
        this.checkAndSetUB(bounds.getUB());
        this.checkAndSetMaxSubsetLowerBoundSubset(bounds.getMaxLowerBoundSubset());
    }

    public boolean canBeSplit(){
        for(Cluster c : this.getClusters()){
            if(c.size() > 1){
                return true;
            }
        }
        return false;
    }

//    Split cluster combination into 'smaller' combinations by replacing the largest cluster with its children
    public ArrayList<ClusterCombination> split(){
        ArrayList<ClusterCombination> subCCs = new ArrayList<>();

        ArrayList<Cluster> clusters = this.getClusters();
        int lSize = LHS.size();
        int rSize = RHS.size();

//        Get cluster with largest radius and more than one point
        int cToBreak = 0;
        double maxRadius = -Double.MAX_VALUE;

        for (int i = 0; i < clusters.size(); i++) {
            Cluster c = clusters.get(i);
            if (c.size() > 1 && c.getRadius() > 0 && c.getRadius() > maxRadius){
                maxRadius = c.getRadius();
                cToBreak = i;
            }
        }

//        Split cluster into children
        Cluster largest = clusters.get(cToBreak);
        boolean isLHS = cToBreak < lSize;

        List<Cluster> newSide = new ArrayList<>(isLHS ? LHS : RHS);
        int newSidePositon = isLHS ? cToBreak : cToBreak - lSize;

//        For each subcluster, create a new cluster combination (unless it is already in the side and is singleton)
        for (Cluster sc : largest.getChildren()) {
            if (newSide.contains(sc) && sc.size() == 1){
                continue;
            }
            newSide.set(newSidePositon, sc);

            ArrayList<Cluster> newLHS = new ArrayList<>(LHS);
            ArrayList<Cluster> newRHS = new ArrayList<>(RHS);
            if (isLHS){
                newLHS = new ArrayList<>(newSide);
            } else {
                newRHS = new ArrayList<>(newSide);
            }
            subCCs.add(new ClusterCombination(newLHS, newRHS));
        }
        return subCCs;
    }

//    Unpack CC to all cluster combinations with singleton clusters
    public ArrayList<ClusterCombination> getSingletons(){
        ArrayList<ClusterCombination> out = new ArrayList<>();
        if (this.canBeSplit()) {
            ArrayList<ClusterCombination> splitted = this.split();
            for (ClusterCombination sc : splitted) {
                out.addAll(sc.getSingletons());
            }
        }else{
            out.add(this);
        }
        return out;
    }

//    Find the maximum similarity of one of the subsets of this cluster combination
    public double getMaxSubsetSimilarity(Parameters par){
        ArrayList<Cluster> subsetSide;
        double subsetSimilarity;
        double maxSim = -Double.MAX_VALUE;

        if (LHS.size() > 1){
            for (int i = 0; i < LHS.size(); i++) {
                subsetSide = new ArrayList<>(LHS);
                subsetSide.remove(i);
                ClusterCombination subCC = new ClusterCombination(subsetSide, RHS);
                subCC.bound(par.simMetric, par.empiricalBounding, par.Wl, par.Wr, par.pairwiseDistances);
                subsetSimilarity = subCC.getLB();
                if (Math.abs(subsetSimilarity - subCC.getUB()) > 0.001){
                    par.LOGGER.fine("Subset similarity is not tight: " + subsetSimilarity + " " + subCC.getUB());
                }

                maxSim = Math.max(maxSim, subsetSimilarity);
                maxSim = Math.max(maxSim, subCC.getMaxSubsetSimilarity(par));
            }
        }

        if (RHS.size() > 1){
            for (int i = 0; i < RHS.size(); i++) {
                subsetSide = new ArrayList<>(RHS);
                subsetSide.remove(i);
                ClusterCombination subCC = new ClusterCombination(LHS, subsetSide);
                subCC.bound(par.simMetric, par.empiricalBounding, par.Wl, par.Wr, par.pairwiseDistances);
                subsetSimilarity = subCC.getLB();
                if (Math.abs(subsetSimilarity - subCC.getUB()) > 0.001){
                    par.LOGGER.fine("Subset similarity is not tight: " + subsetSimilarity + " " + subCC.getUB());
                }

                maxSim = Math.max(maxSim, subsetSimilarity);
                maxSim = Math.max(maxSim, subCC.getMaxSubsetSimilarity(par));
            }
        }
        return maxSim;
    }
}
