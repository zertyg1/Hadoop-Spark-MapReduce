package org.example.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();

            if (line.startsWith("movieId")) {
                return;
            }

            // Diviser seulement sur la première virgule pour garder le titre intact
            int firstComma = line.indexOf(',');
            if (firstComma != -1) {
                String movieId = line.substring(0, firstComma).trim();
                String rest = line.substring(firstComma + 1);
                int secondComma = rest.lastIndexOf(',');
                String title = rest;
                if (secondComma != -1) {
                    title = rest.substring(0, secondComma);
                }
                context.write(new Text(movieId), new Text("MOVIE|" + title.trim()));
            }
        } catch (Exception e) {
            context.getCounter("Error", "Parse Error").increment(1);
        }
    }
}