package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder combinedValues = new StringBuilder();

        for (Text value : values) {
            combinedValues.append(value.toString()).append("; ");
        }

        context.write(key, new Text(combinedValues.toString()));
    }
}