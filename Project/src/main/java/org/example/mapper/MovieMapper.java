package org.example.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("movieId")) return;

        String movieId = line.substring(0, line.indexOf(','));
        String movieName = line.substring(line.indexOf(',') + 1);
        if (movieName.contains(",")) {
            movieName = movieName.substring(0, movieName.lastIndexOf(','));
        }

        context.write(new Text(movieId), new Text("MOVIE|" + movieName.trim()));
    }
}