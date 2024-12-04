package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestRatedMovieWritableMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable> {
    private Text userId = new Text();
    private MovieRatingWritable movieRating = new MovieRatingWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("userId")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length > 3) {
            userId.set(fields[0]); // userId
            String movieId = fields[1];
            double rating = Double.parseDouble(fields[2]);
            movieRating = new MovieRatingWritable(movieId, rating);
            context.write(userId, movieRating);
        }
    }
}
