package algorithms.baselines;

import _aux.*;
import algorithms.Algorithm;
import algorithms.StageRunner;
import core.Parameters;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class Baseline extends Algorithm {
    double[] WlFull;
    double[] WrFull;
    ConcurrentHashMap<Long, Double> similarityCache;
    
    public Baseline(Parameters par) {
        super(par);
        WlFull = par.Wl.get(par.Wl.size()-1);
        WrFull = par.Wr.get(par.Wr.size()-1);
        similarityCache = new ConcurrentHashMap<>((int) Math.pow(par.n, par.maxPLeft + par.maxPRight), .4f);
    }

    public abstract void prepare();

    @Override
    public Set<ResultTuple> run() {
        StageRunner stageRunner = new StageRunner(par.LOGGER);

        //        Start the timer
        par.statBag.stopWatch.start();

        // --> STAGE 1 - Prepare
        stageRunner.run("Preparation phase", this::prepare, par.statBag.stopWatch);

        // --> STAGE 2 - Get candidate pairs
        List<Pair<List<Integer>, List<Integer>>> candidates =
                stageRunner.run("Generate candidates",
                        () -> CandidateGenerator.getCandidates(par.n, par.maxPLeft, par.maxPRight, par.allowSideOverlap,
                                WlFull, WrFull, par.parallel),
                        par.statBag.stopWatch);
        par.LOGGER.info("Number of candidates: " + candidates.size());

        // --> STAGE 3 - Compute similarities
        Set<ResultTuple> results = stageRunner.run("Compute similarities", () -> iterateCandidates(candidates), par.statBag.stopWatch);

        par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        return results;
    }

    
    private Set<ResultTuple> iterateCandidates(List<Pair<List<Integer>, List<Integer>>> candidates){
        return lib.getStream(candidates, par.parallel).flatMap(c ->
            this.assessCandidate(c).stream()
        ).collect(Collectors.toSet());
    }

//    Go over candidate and check if it (or its subsets) has a significant similarity
    private List<ResultTuple> assessCandidate(Pair<List<Integer>, List<Integer>> candidate){
        List<ResultTuple> out = new ArrayList<>();

//        Get own similarity
        double sim = computeSimilarity(candidate.x, candidate.y);

//        Get significant similarities of subsets
        List<Integer> LHS = candidate.x;
        List<Integer> RHS = candidate.y;

//        First LHS
        if (LHS.size() > 1) {
            out.addAll(lib.getStream(IntStream.range(0, LHS.size()).boxed(), par.parallel).flatMap(i -> {
                int j = i; // otherwise remove will remove the object, not the index
                List<Integer> LHSsub = new ArrayList<>(LHS);
                LHSsub.remove(j);
                return assessCandidate(new Pair<>(LHSsub, RHS)).stream();
            }).collect(Collectors.toList()));
        }

//        Then RHS
        if (RHS.size() > 1) {
            out.addAll(lib.getStream(IntStream.range(0, RHS.size()).boxed(), par.parallel).flatMap(i -> {
                int j = i; // otherwise remove will remove the object, not the index
                List<Integer> RHSsub = new ArrayList<>(RHS);
                RHSsub.remove(j);
                return assessCandidate(new Pair<>(LHS, RHSsub)).stream();
            }).collect(Collectors.toList()));
        }

//        Minjump check on direct subsets
        if (sim > par.tau){
            boolean add = true;
//            Iterate over all (direct) significant subsets
            for (ResultTuple subset : out) {
                if (subset.LHS.size() >= LHS.size() - 1 && subset.RHS.size() >= RHS.size() - 1
                        && subset.similarity + par.minJump >= sim) {
                    add = false;
                    break;
                }
            }
            if (add){
//                    Make headers
                List<String> lHeader = LHS.stream().map(i -> par.headers[i]).collect(Collectors.toList());
                List<String> rHeader = RHS.stream().map(i -> par.headers[i]).collect(Collectors.toList());

//                    Add to output
                out.add(new ResultTuple(LHS, RHS, lHeader, rHeader, sim));
            }
        }

        return out;
    }

    public abstract double computeSimilarity(List<Integer> left, List<Integer> right);

    public double[] linearCombination(List<Integer> idx, double[] W){
        double[] v = new double[par.m];
        for (int i = 0; i < idx.size(); i++) {
            v = lib.add(v, lib.scale(par.data[idx.get(0)], W[i]));
        }
        return v;
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
