package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.example.mapper.*;
import org.example.reducer.CountReducer;
import org.example.reducer.GroupReducer;
import org.example.reducer.HighestRatedReducer;
import org.example.reducer.JoinReducer;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Main <ratings_path> <movies_path> <intermediate_path> <final_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.skip.header.line.count", true);

        Path ratingsPath = new Path(args[0]);
        Path moviesPath = new Path(args[1]);
        Path intermediatePath = new Path(args[2]);
        Path finalOutputPath = new Path(args[3]);

        Path phase1Output = new Path(intermediatePath, "phase1");
        if (!runPhase1(conf, ratingsPath, phase1Output)) {
            System.err.println("Phase 1 failed.");
            System.exit(1);
        }

        Path phase2Output = new Path(intermediatePath, "phase2");
        if (!runPhase2(conf, phase1Output, moviesPath, phase2Output)) {
            System.err.println("Phase 2 failed.");
            System.exit(1);
        }

        Path phase3Output = new Path(intermediatePath, "phase3");
        if (!runPhase3(conf, phase2Output, phase3Output)) {
            System.err.println("Phase 3 failed.");
            System.exit(1);
        }

        if (!runPhase4(conf, phase3Output, finalOutputPath)) {
            System.err.println("Phase 4 failed.");
            System.exit(1);
        }

        System.out.println("All phases completed successfully.");
    }

    private static boolean runPhase1(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Phase 1 - Highest Rated Movie");
        job.getConfiguration().setBoolean("mapreduce.input.lineinputformat.skipheader", true);
        job.setJarByClass(Main.class);
        job.setMapperClass(HighestRatedMapper.class);
        job.setReducerClass(HighestRatedReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    private static boolean runPhase2(Configuration conf, Path userMoviePath, Path moviePath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Phase 2 - Join Movies with Names");
        job.setJarByClass(Main.class);
        job.setReducerClass(JoinReducer.class);

        MultipleInputs.addInputPath(job, userMoviePath, TextInputFormat.class, UserMovieMapper.class);
        MultipleInputs.addInputPath(job, moviePath, TextInputFormat.class, MovieMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    private static boolean runPhase3(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Phase 3 - Count Users Per Movie");
        job.setJarByClass(Main.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    private static boolean runPhase4(Configuration conf, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(conf, "Phase 4 - Group by Like Count");
        job.setJarByClass(Main.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }
}
