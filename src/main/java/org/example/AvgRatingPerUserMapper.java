package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgRatingPerUserMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text userId = new Text();
    private DoubleWritable rating = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("userId")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length > 3) {
            userId.set(fields[0]); // userId
            rating.set(Double.parseDouble(fields[2])); // rating
            context.write(userId, rating);
        }
    }
}
