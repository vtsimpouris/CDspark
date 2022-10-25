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

    public Baseline(Parameters par) {
        super(par);
        WlFull = par.Wl.get(par.Wl.size()-1);
        WrFull = par.Wr.size() > 0 ? par.Wr.get(par.Wr.size()-1): null;
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
        par.statBag.otherStats.put("nCandidates", candidates.size());

        // --> STAGE 3 - Compute similarities
        Set<ResultTuple> results = stageRunner.run("Compute similarities", () -> iterateCandidates(candidates), par.statBag.stopWatch);

        par.statBag.stopWatch.stop();
        par.statBag.totalDuration = lib.nanoToSec(par.statBag.stopWatch.getNanoTime());
        par.statBag.stageDurations = stageRunner.stageDurations;
        return results;
    }

    
    private Set<ResultTuple> iterateCandidates(List<Pair<List<Integer>, List<Integer>>> candidates){
        return lib.getStream(candidates, par.parallel)
                .filter(this::assessCandidate)
                .map(c -> new ResultTuple(c.x, c.y, new ArrayList<>(), new ArrayList<>(), par.tau))
                .collect(Collectors.toSet());
    }

//    Go over candidate and check if it (or its subsets) has a significant similarity
    public abstract boolean assessCandidate(Pair<List<Integer>, List<Integer>> candidate);

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
