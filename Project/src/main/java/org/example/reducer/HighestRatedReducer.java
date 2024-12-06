package org.example.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestRatedReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double maxRating = Double.MIN_VALUE;
        String bestMovie = "";

        for (Text value : values) {
            String[] movieRating = value.toString().split(",");
            double rating = Double.parseDouble(movieRating[1]);

            if (rating > maxRating) {
                maxRating = rating;
                bestMovie = movieRating[0];
            }
        }

        context.write(key, new Text(bestMovie));
    }
}