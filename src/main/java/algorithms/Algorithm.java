package algorithms;

import _aux.*;

import java.util.List;

public abstract class Algorithm {
    public Parameters par;

    public Algorithm(Parameters parameters){
        this.par = parameters;
    }

    public abstract List<ResultTuple> run();
    public abstract void printStats(StatBag statBag);

    public void printStageDurations(StatBag statBag){
        lib.printBar(par.LOGGER);
        for (int i = 0; i < statBag.stageDurations.size(); i++) {
            Stage stageDuration = statBag.stageDurations.get(i);

            if (stageDuration.expectedDuration != null){
                par.LOGGER.fine(String.format("Duration stage %d. %-50s: %.5f sec (estimated %.5f sec)",
                        i, stageDuration.name, stageDuration.duration, stageDuration.expectedDuration));
            } else {
                par.LOGGER.fine(String.format("Duration stage %d. %-50s: %.5f sec",
                        i, stageDuration.name, stageDuration.duration));
            }
        }
        par.LOGGER.info(String.format("%-68s: %.5f sec", "Total duration", statBag.totalDuration));
    }


}
