package _aux;

import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StatBag {
    public StopWatch stopWatch = new StopWatch();
    public double totalDuration;
    public List<Stage> stageDurations = new ArrayList<>();




    public void saveStats(Parameters parameters){

        String outputPath = parameters.outputPath;

        try {
            new File(outputPath).mkdirs();

            String fileName = String.format("%s/runs_%s.csv", outputPath, parameters.algorithm);
            File file = new File(fileName);

            boolean exists = file.exists();

//            todo check if complete
            List<String> excludedColumns = Arrays.asList(
                    "LOGGER", "data", "statBag", "saveResults",
                    "resultPath", "headers", "outputPath", "stageDurations",
                    "Wl", "Wr"
            );

            FileWriter resultWriter = new FileWriter(fileName, true);

            parameters.LOGGER.info("saving to " + fileName);

//            Parameter fields
            List<Field> fields = Arrays.stream(parameters.getClass().getDeclaredFields()) // get all attributes of parameter class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());

//            Also get statbag fields
            List<Field> statBagFields = Arrays.stream(this.getClass().getDeclaredFields()) // get all attributes of statbag class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());

            List<Field> allfields = new ArrayList<>(fields);
            allfields.addAll(statBagFields);

//            Do stage durations last
            allfields.add(this.getClass().getField("stageDurations"));

//            Create header
            if (!exists) {
                resultWriter.write(allfields.stream().map(Field::getName).collect(Collectors.joining(",")) + "\n");
            }

//            Write results out in one row
            String paramRow = fields.stream().map(field -> {
                try {
                    return String.valueOf(field.get(parameters));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    return "";
                }
            }).collect(Collectors.joining(","));

            String statBagRow = statBagFields.stream().map(field -> {
                try {
                    return String.valueOf(field.get(this));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    return "";
                }
            }).collect(Collectors.joining(","));

//            Add stagedurations as nested list
            String sdString = this.stageDurations.stream().map(st -> String.valueOf(st.getDuration())).collect(Collectors.joining("-"));

            resultWriter.write(paramRow + "," + statBagRow + "," + sdString + "\n");

            resultWriter.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
