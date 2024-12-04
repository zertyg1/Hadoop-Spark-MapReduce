package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgRatingPerMovieMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text movieId = new Text();
    private DoubleWritable rating = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("movieId")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length > 3) {
            movieId.set(fields[1]); // movieId
            rating.set(Double.parseDouble(fields[2])); // rating
            context.write(movieId, rating);
        }
    }
}
