package org.example.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String movieName = "";
        List<String> users = new ArrayList<>();

        for (Text value : values) {
            String[] parts = value.toString().split("\\|");
            if (parts[0].equals("MOVIE")) {
                movieName = parts[1];
            } else if (parts[0].equals("USER")) {
                users.add(parts[1]);
            }
        }

        for (String user : users) {
            context.write(new Text(user), new Text(movieName));
        }
    }
}