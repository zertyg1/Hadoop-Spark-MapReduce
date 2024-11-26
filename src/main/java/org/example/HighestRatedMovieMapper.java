package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestRatedMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text userId = new Text();
    private Text movieRating = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("userId")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length > 3) {
            userId.set(fields[0]); // userId
            String movieId = fields[1];
            String rating = fields[2];
            movieRating.set(movieId + "," + rating);
            context.write(userId, movieRating);
        }
    }
}
