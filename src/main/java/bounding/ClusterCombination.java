package bounding;

import _aux.Parameters;
import _aux.ResultTuple;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import similarities.MultivariateSimilarityFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterCombination {
    ArrayList<Cluster> LHS;
    ArrayList<Cluster> RHS;

    @Setter @Getter boolean isPositive = false;
    @Setter @Getter boolean isDecisive = false;
    Boolean isSingleton;
    private List<Cluster> clusters;

    @Getter double LB = -Double.MAX_VALUE;
    @Getter double UB = Double.MAX_VALUE;
    @Getter double maxLowerBoundSubset = -Double.MAX_VALUE;


    Double maxSubsetSimilarity;



//    ------------------- METHODS -------------------
    public ClusterCombination(@NonNull ArrayList<Cluster> LHS, @NonNull ArrayList<Cluster> RHS) {
        this.LHS = LHS;
        this.RHS = RHS;
    }

    public int size(){
        return this.getClusters().stream().mapToInt(Cluster::size).sum();
    }

    public boolean isSingleton(){
        if (this.isSingleton == null){
            for (Cluster c : this.getClusters()){
                if (c.size() > 1){
                    this.isSingleton = false;
                    return false;
                }
            }
            this.isSingleton = true;
        }
        return this.isSingleton;
    }

    public List<Cluster> getClusters(){
        if (this.clusters == null){
            this.clusters = new ArrayList<>();
            this.clusters.addAll(LHS);
            this.clusters.addAll(RHS);
        }
        return this.clusters;
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

//    Split cluster combination into 'smaller' combinations by replacing the largest cluster with its children
    public ArrayList<ClusterCombination> split(){
        ArrayList<ClusterCombination> subCCs = new ArrayList<>();

        int lSize = LHS.size();

//        Get cluster with largest radius and more than one point
        int cToBreak = 0;
        double maxRadius = -Double.MAX_VALUE;

        for (int i = 0; i < this.getClusters().size(); i++) {
            Cluster c = this.getClusters().get(i);
            if (c.size() > 1 && c.getRadius() > 0 && c.getRadius() > maxRadius){
                maxRadius = c.getRadius();
                cToBreak = i;
            }
        }

//        Split cluster into children

        boolean isLHS = cToBreak < lSize;
        ArrayList<Cluster> newSide = new ArrayList<>(isLHS ? LHS : RHS);
        int newSidePosition = isLHS ? cToBreak : cToBreak - lSize;

        Cluster largest = newSide.remove(newSidePosition);

//        For each subcluster, create a new cluster combination (unless it is already in the side and is singleton)
        for (Cluster sc : largest.getChildren()) {
            if (newSide.contains(sc) && sc.size() == 1){
                continue;
            }
            newSide.add(newSidePosition, sc);

            ArrayList<Cluster> newLHS = new ArrayList<>(LHS);
            ArrayList<Cluster> newRHS = new ArrayList<>(RHS);
            if (isLHS){
                newLHS = new ArrayList<>(newSide);
            } else {
                newRHS = new ArrayList<>(newSide);
            }
            subCCs.add(new ClusterCombination(newLHS, newRHS));

            // remove the subcluster to make room for the next subcluster
            newSide.remove(newSidePosition);
            if(newSide.contains(sc)){
                break;
            }
        }
        return subCCs;
    }

//    Unpack CC to all cluster combinations with singleton clusters
    public ArrayList<ClusterCombination> getSingletons(){
        ArrayList<ClusterCombination> out = new ArrayList<>();
        if (!this.isSingleton()) {
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
        if (maxSubsetSimilarity == null){
            ArrayList<Cluster> subsetSide;
            double subsetSimilarity;
            maxSubsetSimilarity = -Double.MAX_VALUE;

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

                    maxSubsetSimilarity = Math.max(maxSubsetSimilarity, subsetSimilarity);
                    maxSubsetSimilarity = Math.max(maxSubsetSimilarity, subCC.getMaxSubsetSimilarity(par));
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

                    maxSubsetSimilarity = Math.max(maxSubsetSimilarity, subsetSimilarity);
                    maxSubsetSimilarity = Math.max(maxSubsetSimilarity, subCC.getMaxSubsetSimilarity(par));
                }
            }
        }

        return maxSubsetSimilarity;
    }

    public ResultTuple toResultTuple(String[] headers){
//        Check if singleton, otherwise raise error
        List<Integer> LHSIndices = LHS.stream().map(c -> c.pointsIdx.get(0)).collect(Collectors.toList());
        List<Integer> RHSIndices = RHS.stream().map(c -> c.pointsIdx.get(0)).collect(Collectors.toList());

        if (this.isSingleton()){
            return new ResultTuple(
                    LHSIndices,
                    RHSIndices,
                    LHSIndices.stream().map(i -> headers[i]).collect(Collectors.toList()),
                    RHSIndices.stream().map(i -> headers[i]).collect(Collectors.toList()),
                    this.getLB()
            );
        } else {
            throw new IllegalArgumentException("Cluster combination is not a singleton");
        }

    }
}
