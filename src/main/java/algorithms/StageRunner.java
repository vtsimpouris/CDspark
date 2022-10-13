package algorithms;

import _aux.Stage;
import _aux.Tuple3;
import _aux.lib;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Logger;

@RequiredArgsConstructor
public class StageRunner {
    private final Logger LOGGER;

//    <Name, Duration, ExpectedDuration>
    public final List<Stage> stageDurations = new ArrayList<>();

    public <T> T run(String name, Supplier<T> stage, StopWatch stopWatch) {
        LOGGER.fine(String.format("----------- %d. %s --------------",stageDurations.size(), name));

        try {
            return stage.get();
        } finally {
            stopWatch.split();
            double splitTime = lib.nanoToSec(stopWatch.getSplitNanoTime());
            double splitDuration = stageDurations.isEmpty() ? splitTime : splitTime - stageDurations.get(stageDurations.size() - 1).getDuration();
            stageDurations.add(new Stage(name, splitDuration));
        }

    }

    public void run(String name, Runnable stage, StopWatch stopWatch) {
        LOGGER.fine(String.format("----------- %d. %s --------------",stageDurations.size(), name));

        long start = System.currentTimeMillis();
        try {
            stage.run();
        } finally {
            stopWatch.split();
            double splitTime = lib.nanoToSec(stopWatch.getSplitNanoTime());
            double splitDuration = stageDurations.isEmpty() ? splitTime : splitTime - stageDurations.get(stageDurations.size() - 1).getDuration();
            stageDurations.add(new Stage(name, splitDuration));
        }
    }
}
