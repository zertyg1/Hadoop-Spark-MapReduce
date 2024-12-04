package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestRatedMovieWritableReducer extends Reducer<Text, MovieRatingWritable, Text, MovieRatingWritable> {
    public void reduce(Text key, Iterable<MovieRatingWritable> values, Context context) throws IOException, InterruptedException {
        MovieRatingWritable highestRatedMovie = null;

        for (MovieRatingWritable value : values) {
            if (highestRatedMovie == null || value.getRating() > highestRatedMovie.getRating()) {
                highestRatedMovie = new MovieRatingWritable(value.getMovieId(), value.getRating());
            }
        }

        if (highestRatedMovie != null) {
            context.write(key, highestRatedMovie);
        }
    }
}
