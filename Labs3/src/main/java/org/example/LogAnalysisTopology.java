package org.example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class LogAnalysisTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("log-spout", new LogSpout());
        builder.setBolt("count-attempts-bolt", new CountAttemptsBolt()).shuffleGrouping("log-spout");
        builder.setBolt("ip-attempts-bolt", new IPAttemptsBolt()).shuffleGrouping("log-spout");
        builder.setBolt("username-popularity-bolt", new UsernamePopularityBolt()).shuffleGrouping("log-spout");

        Config config = new Config();
        config.setDebug(true);

        StormSubmitter.submitTopology("LogAnalysisTopology", config, builder.createTopology());
    }
}
