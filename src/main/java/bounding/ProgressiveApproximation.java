package bounding;

import _aux.lib;
import core.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProgressiveApproximation {

    public static List<ClusterCombination> ApproximateProgressively(List<ClusterCombination> approximatedCCs, List<ClusterCombination> runningPosDCCs, Parameters par){
        if (approximatedCCs.size() == 0){return runningPosDCCs;}

        // First group the approximated cluster combinations by their critical shrinkfactor
        Map<Integer, List<ClusterCombination>> priorityBuckets = approximatedCCs.stream().unordered()
                .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), par.nPriorityBuckets)));

        // Now iterate over the buckets with approximated DCCs, start with highest priority (i.e. lowest critical shrinkfactor)
        for(int key = 1; key<= par.nPriorityBuckets; key++) {
            List<ClusterCombination> priorityDCCs = priorityBuckets.remove(key);

            //continue to the next bucket if this one does not contain any DCCs to process
            if (priorityDCCs == null) continue;

            // All priority buckets that come from here should have lower priority (i.e. higher id) then the key currently being processed
            double runningShrinkFactor = 1;
            if (par.approximationStrategy.equals(ApproximationStrategyEnum.INCREMENTAL)) {
//                Set incremental shrinkFactor
                runningShrinkFactor = Math.min((double) key / par.nPriorityBuckets + 0.1, 1);
            }
            final double finalRunningShrinkFactor = runningShrinkFactor;

            Map<Integer, List<ClusterCombination>> processedDCCs = lib.getStream(priorityDCCs, par.parallel).unordered()
                    .flatMap(cc -> (RecursiveBounding.recursiveBounding(cc, finalRunningShrinkFactor, par)).stream())
                    .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), par.nPriorityBuckets)));

//            Filter out the found extra posDCCs, reallocate negative DCCs to new bucket
            List<ClusterCombination> additionalPositives = new ArrayList<>(100);
            lib.getStream(processedDCCs.keySet(), par.parallel).unordered()
                    .forEach(newKey -> {
                        if (newKey <= 1){ // positive DCCs
                            additionalPositives.addAll(processedDCCs.get(newKey));
                        } else if (newKey < par.nPriorityBuckets + 1){ // negative DCCs to be reallocated
                            priorityBuckets.merge(newKey, processedDCCs.get(newKey), (oldList, newList) -> {
                                oldList.addAll(newList);
                                return oldList;
                            });
                        }
                    });

//            Update the positive DCCs
            runningPosDCCs.addAll(RecursiveBounding.unpackAndCheckMinJump(additionalPositives, par));
            runningPosDCCs = RecursiveBounding.updateTopK(runningPosDCCs, par);
        }

        return runningPosDCCs;
    }

    private static int getPriorityBucket(double criticalShrinkFactor, int nBuckets){
        return (int) Math.ceil((Math.min(criticalShrinkFactor, 1) * nBuckets));
    }
}
