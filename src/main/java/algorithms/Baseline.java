package algorithms;

import _aux.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Baseline extends Algorithm{
    double[] WlFull;
    double[] WrFull;
    
    public Baseline(Parameters par) {
        super(par);
        WlFull = par.Wl.get(par.Wl.size()-1);
        WrFull = par.Wr.get(par.Wr.size()-1);
    }

    @Override
    public List<ResultTuple> run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);

        //        Start the timer
        par.statBag.stopWatch.start();

//        Test which getCandidates is faster
        List<Pair<List<Integer>, List<Integer>>> candidates = stageRunner.run("Get candidates1", this::getCandidates, par.statBag.stopWatch);
        par.LOGGER.info("Number of candidates: " + candidates.size());

        List<Pair<List<Integer>, List<Integer>>> candidates2 = stageRunner.run("Get candidates2", this::getCandidates2, par.statBag.stopWatch);
        par.LOGGER.info("Number of candidates: " + candidates2.size());

        par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        return new ArrayList<>();
    }

    
    private List<ResultTuple> findSimilarities(){
        return null;
    }

    private List<List<Integer>> getCandidatesSide(boolean left){
        double[] WFull = left ? WlFull : WrFull;
        int maxSize = left ? par.maxPLeft : par.maxPRight;

        List<List<Integer>> baseCandidates = new ArrayList<>(par.n);
        for (int i = 0; i < par.n; i++) {
            baseCandidates.add(new ArrayList<>(Collections.singletonList(i)));
        }

        for (int i = 1; i < maxSize; i++) {
            baseCandidates = lib.getStream(baseCandidates, par.parallel)
                    .flatMap(c -> IntStream.range(0, par.n)
                            .mapToObj(j -> {
                                List<Integer> newC = new ArrayList<>(c);
                                newC.add(j);
                                return newC;
                            })
                            .filter(newC -> !this.isDuplicateOneSide(newC, WFull)))
                    .collect(Collectors.toList());
        }

//        for (int i = 1; i < maxSize; i++) {
//            List<List<Integer>> newCandidates = new ArrayList<>(baseCandidates.size() * par.n);
//            for (List<Integer> comb : baseCandidates) {
//                for (int j = 0; j < par.n; j++) {
//                    List<Integer> newComb = new ArrayList<>(comb);
//                    newComb.add(j);
//                    if(!this.isDuplicateOneSide(newComb, WFull)){
//                        newCandidates.add(newComb);
//                    }
//                }
//            }
//            baseCandidates = newCandidates;
//        }

        return baseCandidates;
    }

    //        Get all permutations of vector indices of size maxPLeft + maxPRight
    private List<Pair<List<Integer>, List<Integer>>> getCandidates(){
        List<List<Integer>> candidatesLeft = getCandidatesSide(true);
        List<List<Integer>> candidatesRight = getCandidatesSide(false);

        List<Pair<List<Integer>, List<Integer>>> candidates = lib.getStream(candidatesLeft, par.parallel)
                .flatMap(l -> lib.getStream(candidatesRight, par.parallel)
                        .filter(r -> !this.isDuplicateTwoSide(l,r))
                        .map(r -> new Pair<>(l, r)))
                .collect(Collectors.toList());

//        List<Pair<List<Integer>, List<Integer>>> candidates = new ArrayList<>(candidatesLeft.size() * candidatesRight.size());
//        for (List<Integer> combLeft : candidatesLeft) {
//            for (List<Integer> combRight : candidatesRight) {
//                if (!this.isDuplicateTwoSide(combLeft, combRight)) {
//                    candidates.add(new Pair<>(combLeft, combRight));
//                }
//            }
//        }

        return candidates;
    }

//    Tested speed, but is slower than getCandidates
    private List<Pair<List<Integer>, List<Integer>>> getCandidates2(){
        int nCombs = (int) Math.pow(par.n, par.maxPLeft + par.maxPRight);
        List<Pair<List<Integer>, List<Integer>>> candidates =
                lib.getStream(IntStream.range(0,nCombs).boxed(), par.parallel).unordered().map(i -> {
                    List<Integer> LHS = new ArrayList<>(par.maxPLeft);
                    List<Integer> RHS = new ArrayList<>(par.maxPRight);

                    for (int k = 0; k < par.maxPLeft; k++) {
                        LHS.add((int) (i / Math.pow(par.n, k)) % par.n);
                    }
                    for (int k = 0; k < par.maxPRight; k++) {
                        RHS.add((int) (i / Math.pow(par.n, k + par.maxPLeft)) % par.n);
                    }
                    if (!this.isDuplicateFull(LHS, RHS)) {
                        return new Pair<>(LHS, RHS);
                    } else {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList());

//        for (int i = 0; i < nCombs; i++) {
//            List<Integer> LHS = new ArrayList<>(par.maxPLeft);
//            List<Integer> RHS = new ArrayList<>(par.maxPRight);
//
//            for (int k = 0; k < par.maxPLeft; k++) {
//                LHS.add((int) (i / Math.pow(par.n, k)) % par.n);
//            }
//            for (int k = 0; k < par.maxPRight; k++) {
//                RHS.add((int) (i / Math.pow(par.n, k + par.maxPLeft)) % par.n);
//            }
//
//            if (!this.isDuplicateFull(LHS, RHS)){
//                candidates.add(new Pair<>(LHS, RHS));
//            }
//        }
        return candidates;
    }

    private boolean isDuplicateOneSide(List<Integer> candidateSide, double[] WFull){
        for (int i = 0; i < candidateSide.size(); i++) {
            for (int j = i + 1; j < candidateSide.size(); j++) {
                if(candidateSide.get(i) >= candidateSide.get(j) && WFull[i] == WFull[j]){
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isDuplicateTwoSide(List<Integer> left, List<Integer> right){
        for (int i = 0; i < left.size(); i++) {
            if (right.contains(left.get(i))){
                return true;
            }
        }
        return false;
    }

    private boolean isDuplicateFull(List<Integer> left, List<Integer> right){
//        Check LHS for duplicates
        for (int i = 0; i < par.maxPLeft; i++) {
//            Vectors cannot be on both sides
            if (left.contains(right.get(i))) {
                return true;
            }
//            Use combinations if weights are equal per position in side
            for (int j = i+1; j < par.maxPLeft; j++) {
                if (left.get(i) >= left.get(j) && WlFull[i] >= WlFull[j]) {
                    return true;
                }
            }
        }

//        Do the same for RHS
        for (int i = 0; i < par.maxPRight; i++) {
//            Use combinations if weights are equal per position in side
            for (int j = i+1; j < par.maxPRight; j++) {
                if (right.get(i) >= right.get(j) && WrFull[i] >= WrFull[j]) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void prepareStats(){

    }

    @Override
    public void printStats(StatBag statBag){
        par.LOGGER.fine("----------- Run statistics --------------");
        this.printStageDurations(statBag);
    }
}
