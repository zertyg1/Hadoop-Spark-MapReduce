package org.example;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class UsernamePopularityBolt extends BaseBasicBolt {
    private Map<String, Integer> usernameCounts = new HashMap<>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String logLine = input.getStringByField("logLine");
        String username = extractUsername(logLine);
        if (username != null) {
            usernameCounts.put(username, usernameCounts.getOrDefault(username, 0) + 1);
        }
        System.out.println("[INFO] Username counts: " + usernameCounts);
    }

    private String extractUsername(String logLine) {
        try {
            return logLine.split("for")[1].split(" ")[1];
        } catch (Exception e) {
            System.err.println("Error extracting username from line: " + logLine);
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Aucun champ émis
    }
}
