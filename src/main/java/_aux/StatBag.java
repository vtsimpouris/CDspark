package _aux;

import core.Parameters;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StatBag implements Serializable {
    public StopWatch stopWatch = new StopWatch();
    public double totalDuration;
    public long nResults;
    public List<Stage> stageDurations = new ArrayList<>();

//    Algorithm specific stats
    public transient AtomicLong nCCs = new AtomicLong(0);
    public AtomicLong totalCCSize = new AtomicLong(0);
    public AtomicLong nNegDCCs = new AtomicLong(0);
    public HashMap<String, Object> otherStats = new HashMap<>();

    public void addStat(String key, Object value){
        otherStats.put(key, value);
    }


    public void saveStats(Parameters parameters){

        String outputPath = parameters.outputPath;

        try {
            new File(outputPath).mkdirs();

            String fileName = String.format("%s/runs_%s.csv", outputPath, parameters.algorithm);
            File file = new File(fileName);

            boolean exists = file.exists();

            List<String> excludedColumns = Arrays.asList(
                    "LOGGER", "data", "statBag", "saveResults",
                    "resultPath", "headers", "outputPath", "stageDurations",
                    "Wl", "Wr", "otherStats", "stopWatch", "pairwiseDistances", "randomGenerator"
            );

            FileWriter resultWriter = new FileWriter(fileName, true);

            parameters.LOGGER.info("saving to " + fileName);

//            Parameter fields
            List<Field> paramFields = Arrays.stream(parameters.getClass().getDeclaredFields()) // get all attributes of parameter class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());

//            Also get statbag fields
            List<Field> statBagFields = Arrays.stream(this.getClass().getDeclaredFields()) // get all attributes of statbag class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());


//            Create k-v store for all fields
            ConcurrentHashMap<String, Object> fieldMap = new ConcurrentHashMap<>();
            paramFields.forEach(field -> {
                try {
                    fieldMap.put(field.getName(), field.get(parameters));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
            statBagFields.forEach(field -> {
                try {
                    fieldMap.put(field.getName(), field.get(this));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });

//            Add otherStats manually
            fieldMap.putAll(otherStats);

//            Add stagedurations as nested list
            String sdString = this.stageDurations.stream().map(st -> String.valueOf(st.getDuration())).collect(Collectors.joining("-"));
            fieldMap.put("stageDurations", sdString);

//            Create header
            if (!exists) {
                String header = fieldMap.keySet().stream().collect(Collectors.joining(","));
                resultWriter.write(header + "\n");
            }

//            Create row
            String row = fieldMap.values().stream().map(Object::toString).collect(Collectors.joining(","));
            resultWriter.write(row + "\n");
            resultWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
