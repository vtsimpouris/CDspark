package bounding;

import core.Parameters;
import _aux.ResultTuple;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import similarities.MultivariateSimilarityFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ClusterCombination {
    @NonNull ArrayList<Cluster> LHS;
    @NonNull ArrayList<Cluster> RHS;
    @NonNull int level;

    @Setter @Getter boolean isPositive = false;
    @Setter @Getter boolean isDecisive = false;
    Boolean isSingleton;
    private List<Cluster> clusters;

    @Getter double LB = -Double.MAX_VALUE;
    @Getter double UB = Double.MAX_VALUE;
    @Getter double maxLowerBoundSubset = -Double.MAX_VALUE;


    Double maxSubsetSimilarity;



//    ------------------- METHODS -------------------
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
            bounds = simMetric.empiricalSimilarityBounds(LHS, RHS, Wl, Wr, pairwiseDistances);
        } else {
            bounds = simMetric.theoreticalSimilarityBounds(LHS, RHS, Wl, Wr);
        }
        this.checkAndSetLB(bounds.getLB());
        this.checkAndSetUB(bounds.getUB());
        this.checkAndSetMaxSubsetLowerBoundSubset(bounds.getMaxLowerBoundSubset());
    }

//    Split cluster combination into 'smaller' combinations by replacing the largest cluster with its children
    public ArrayList<ClusterCombination> split(double[] Wl, double[] Wr, boolean allowSideOverlap){
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

        boolean isLHS = cToBreak < lSize;
        ArrayList<Cluster> newSide = new ArrayList<>(isLHS ? LHS : RHS);
        int newSidePosition = isLHS ? cToBreak : cToBreak - lSize;
        ArrayList<Cluster> otherSide = isLHS ? RHS : LHS;

//        Cluster to split
        Cluster largest = newSide.remove(newSidePosition);

//        For each subcluster, create a new cluster combination  (considering potential sideOverlap and weightOverlap)
        for (Cluster sc : largest.getChildren()) {
            if (sc.size() == 1 &&
                    ((!allowSideOverlap && otherSide.contains(sc)) || // side overlap
                            weightOverlap(sc, newSidePosition, isLHS ? LHS: RHS, isLHS ? Wl: Wr)) // weight overlap
            ){
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
            subCCs.add(new ClusterCombination(newLHS, newRHS, this.level + 1));

            // remove the subcluster to make room for the next subcluster
            newSide.remove(newSidePosition);
        }
        return subCCs;
    }

    private boolean weightOverlap(Cluster cToAdd, int posToAdd, List<Cluster> sideToAdd, double[] weights){
        for (int i = 0; i < sideToAdd.size(); i++) {
            if (i == posToAdd){
                continue;
            }
            Cluster c = sideToAdd.get(i);
            if (weights[posToAdd] == weights[i] && c.id <= cToAdd.id){
                return true;
            }
        }
        return false;
    }

//    Unpack CC to all cluster combinations with singleton clusters
    public ArrayList<ClusterCombination> getSingletons(double[] Wl, double[] Wr, boolean allowSideOverlap){
        ArrayList<ClusterCombination> out = new ArrayList<>();
        if (!this.isSingleton()) {
            ArrayList<ClusterCombination> splitted = this.split(Wl, Wr, allowSideOverlap);
            for (ClusterCombination sc : splitted) {
                out.addAll(sc.getSingletons(Wl, Wr, allowSideOverlap));
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
                    ClusterCombination subCC = new ClusterCombination(subsetSide, RHS, level);
                    subCC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(subsetSide.size() - 1),
                            subCC.RHS.size() > 0 ? par.Wr.get(subCC.RHS.size() - 1): null, par.pairwiseDistances);
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
                    ClusterCombination subCC = new ClusterCombination(LHS, subsetSide, level);
                    subCC.bound(par.simMetric, par.empiricalBounding, par.Wl.get(LHS.size() - 1),
                            par.Wr.get(subsetSide.size() - 1), par.pairwiseDistances);
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
