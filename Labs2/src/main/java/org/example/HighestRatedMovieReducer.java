package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestRatedMovieReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String highestRatedMovie = null;
        double highestRating = Double.MIN_VALUE;

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String movieId = fields[0];
            double rating = Double.parseDouble(fields[1]);

            if (rating > highestRating) {
                highestRating = rating;
                highestRatedMovie = movieId;
            }
        }

        context.write(key, new Text(highestRatedMovie + "," + highestRating));
    }
}
