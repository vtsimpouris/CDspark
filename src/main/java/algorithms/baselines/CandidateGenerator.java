package algorithms.baselines;

import _aux.Pair;
import _aux.lib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CandidateGenerator {
    //        Get all permutations of vector indices of size maxPLeft + maxPRight
    public static List<Pair<List<Integer>, List<Integer>>> getCandidates(int n, int maxPLeft, int maxPRight,
                                                                         double[] WlFull, double[] WrFull,
                                                                         boolean parallel) {
        List<List<Integer>> candidatesLeft = getCandidatesSide(n, maxPLeft, WlFull, parallel);
        List<List<Integer>> candidatesRight = getCandidatesSide(n, maxPRight, WrFull, parallel);

        List<Pair<List<Integer>, List<Integer>>> candidates = lib.getStream(candidatesLeft, parallel)
                .flatMap(l -> lib.getStream(candidatesRight, parallel)
                        .filter(r -> !isDuplicateTwoSide(l,r))
                        .map(r -> new Pair<>(l, r)))
                .collect(Collectors.toList());
        return candidates;
    }

    private static List<List<Integer>> getCandidatesSide(int n, int maxP, double[] weights, boolean parallel){
        if (weights.length < maxP) {
            throw new IllegalArgumentException("Not enough weights for maxP");
        }

        List<List<Integer>> baseCandidates = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            baseCandidates.add(new ArrayList<>(Collections.singletonList(i)));
        }

        for (int i = 1; i < maxP; i++) {
            baseCandidates = lib.getStream(baseCandidates, parallel)
                    .flatMap(c -> IntStream.range(0, n)
                            .mapToObj(j -> {
                                List<Integer> newC = new ArrayList<>(c);
                                newC.add(j);
                                return newC;
                            })
                            .filter(newC -> !isDuplicateOneSide(newC, weights)))
                    .collect(Collectors.toList());
        }
        return baseCandidates;
    }

    public static boolean isDuplicateOneSide(List<Integer> candidateSide, double[] weights){
        for (int i = 0; i < candidateSide.size(); i++) {
            for (int j = i + 1; j < candidateSide.size(); j++) {
                if(candidateSide.get(i) >= candidateSide.get(j) && weights[i] == weights[j]){
                    return true;
                }
            }
        }
        return false;
    }

    //    Check if overlap between two sides
    public static boolean isDuplicateTwoSide(List<Integer> left, List<Integer> right){
        for (int i = 0; i < left.size(); i++) {
            if (right.contains(left.get(i))){
                return true;
            }
        }
        return false;
    }

    public static boolean isDuplicateFull(List<Integer> left, List<Integer> right, double[] Wl, double[] Wr){
//        Check LHS for duplicates
        for (int i = 0; i < left.size(); i++) {
//            Vectors cannot be on both sides
            if (left.contains(right.get(i))) {
                return true;
            }
//            Use combinations if weights are equal per position in side
            for (int j = i+1; j < right.size(); j++) {
                if (left.get(i) >= left.get(j) && Wl[i] >= Wl[j]) {
                    return true;
                }
            }
        }

//        Do the same for RHS
        for (int i = 0; i < left.size(); i++) {
//            Use combinations if weights are equal per position in side
            for (int j = i+1; j < right.size(); j++) {
                if (right.get(i) >= right.get(j) && Wl[i] >= Wr[j]) {
                    return true;
                }
            }
        }
        return false;
    }


//    ----------------------------------- archive ----------------------------------

//    //    Tested speed, but is slower than getCandidates
//    private List<Pair<List<Integer>, List<Integer>>> getCandidates2(){
//        int nCombs = (int) Math.pow(par.n, par.maxPLeft + par.maxPRight);
//        List<Pair<List<Integer>, List<Integer>>> candidates =
//                lib.getStream(IntStream.range(0,nCombs).boxed(), par.parallel).unordered().map(i -> {
//                    List<Integer> LHS = new ArrayList<>(par.maxPLeft);
//                    List<Integer> RHS = new ArrayList<>(par.maxPRight);
//
//                    for (int k = 0; k < par.maxPLeft; k++) {
//                        LHS.add((int) (i / Math.pow(par.n, k)) % par.n);
//                    }
//                    for (int k = 0; k < par.maxPRight; k++) {
//                        RHS.add((int) (i / Math.pow(par.n, k + par.maxPLeft)) % par.n);
//                    }
//                    if (!this.isDuplicateFull(LHS, RHS)) {
//                        return new Pair<>(LHS, RHS);
//                    } else {
//                        return null;
//                    }
//                }).filter(Objects::nonNull).collect(Collectors.toList());
//        return candidates;
//    }

    //    Check if we can allow to filter some candidates because of overlapping weights (e.g. 0.5-0.5-1)

}
