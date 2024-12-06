package org.example.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> movies = new ArrayList<>();
        for (Text value : values) {
            movies.add(value.toString());
        }

        context.write(key, new Text(String.join(" ", movies)));
    }
}