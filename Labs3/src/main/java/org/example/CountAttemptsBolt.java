package org.example;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class CountAttemptsBolt extends BaseBasicBolt {
    private int totalAttempts = 0;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        totalAttempts++;
        if (totalAttempts % 5 == 0) {
            System.out.println("[INFO] Total login attempts: " + totalAttempts);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Aucun champ émis
    }
}
