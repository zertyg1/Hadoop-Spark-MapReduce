package org.example;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class IPAttemptsBolt extends BaseBasicBolt {
    private Map<String, Integer> ipCounts = new HashMap<>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String logLine = input.getStringByField("logLine");
        String ip = extractIP(logLine);
        if (ip != null) {
            ipCounts.put(ip, ipCounts.getOrDefault(ip, 0) + 1);
        }

        if (ipCounts.size() % 5 == 0) {
            System.out.println("[INFO] IP Attempt Counts: " + ipCounts);
        }
    }

    private String extractIP(String logLine) {
        try {
            return logLine.split("from")[1].split(" ")[1];
        } catch (Exception e) {
            System.err.println("Error extracting IP from line: " + logLine);
            return null;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Aucun champ émis
    }
}
