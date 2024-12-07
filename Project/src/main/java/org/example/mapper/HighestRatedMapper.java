package org.example.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestRatedMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();

            // Ignorer l'en-tête
            if (line.startsWith("userId")) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length >= 3) {
                int userId = Integer.parseInt(fields[0].trim());
                String movieId = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());

                if (rating > 0.0) { // Vérifier que la note est valide
                    context.write(new IntWritable(userId),
                            new Text(movieId + "," + rating));
                }
            }
        } catch (Exception e) {
            context.getCounter("Error", "Parse Error").increment(1);
        }
    }
}