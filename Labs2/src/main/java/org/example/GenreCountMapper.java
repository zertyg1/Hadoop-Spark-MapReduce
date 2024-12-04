package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenreCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text genre = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("movieId")) {
            return;
        }

        String[] fields = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        if (fields.length > 2) {
            String[] genres = fields[2].split("\\|");
            for (String g : genres) {
                genre.set(g);
                context.write(genre, one);
            }
        }
    }
}
