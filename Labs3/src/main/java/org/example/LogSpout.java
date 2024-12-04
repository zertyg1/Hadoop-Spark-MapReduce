package org.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class LogSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private String filePath = "/storm/labs/SSH.log";

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            reader = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {
            System.err.println("Error opening file: " + filePath);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                collector.emit(new Values(line));
            } else {
                Thread.sleep(100); // Pause pour éviter une boucle infinie sur EOF
            }
        } catch (Exception e) {
            System.err.println("Error reading line: " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("logLine"));
    }

    @Override
    public void close() {
        try {
            if (reader != null) reader.close();
        } catch (IOException e) {
            System.err.println("Error closing file: " + e.getMessage());
        }
    }
}
