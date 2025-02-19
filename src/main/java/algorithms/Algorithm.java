package algorithms;

import _aux.*;
import core.Parameters;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public abstract class Algorithm implements Serializable {
    public transient Parameters par;

    public Algorithm(Parameters parameters){
        this.par = parameters;
    }

    public abstract Set<ResultTuple> run();
    public abstract void printStats(StatBag statBag);
    public abstract void prepareStats();

    public void printStageDurations(StatBag statBag){
        lib.printBar(par.LOGGER);
        for (int i = 0; i < statBag.stageDurations.size(); i++) {
            Stage stageDuration = statBag.stageDurations.get(i);
            // bug fix for Hierarchical clustering duration (sometimes system gives negative duration)
            if(stageDuration.duration < 0){
                stageDuration.duration = -stageDuration.duration;
            }

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
