package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MoviesByGenreReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder movies = new StringBuilder();

        for (Text movie : values) {
            movies.append(movie.toString()).append(", ");
        }

        // Remove the last comma and space
        if (movies.length() > 0) {
            movies.setLength(movies.length() - 2);
        }

        context.write(key, new Text(movies.toString()));
    }
}
