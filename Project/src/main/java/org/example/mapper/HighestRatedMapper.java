package org.example.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestRatedMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length < 3) return;

        int userId = Integer.parseInt(fields[0]);
        String movieId = fields[1];
        double rating = Double.parseDouble(fields[2]);

        context.write(new IntWritable(userId), new Text(movieId + "," + rating));
    }
}