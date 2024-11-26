package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MoviesByGenreMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text genreKey = new Text();
    private Text movieTitle = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("movieId")) {
            return;
        }

        String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        if (fields.length > 2) {
            String movieName = fields[1];
            String[] genres = fields[2].split("\\|");

            for (String genre : genres) {
                genreKey.set(genre);
                movieTitle.set(movieName);
                context.write(genreKey, movieTitle);
            }
        }
    }
}
