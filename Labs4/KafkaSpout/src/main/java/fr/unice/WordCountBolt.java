package fr.unice;

import org.apache.storm.topology.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
  private Map<String, Integer> wordCount = new HashMap<>();

  @Override
  public void prepare(Map stormConf, org.apache.storm.task.TopologyContext context) {
    // Initialize any necessary resources
  }

  @Override
  public void execute(Tuple input) {
    // Get the message (sentence) from Kafka
    String sentence = input.getStringByField("sentence");

    // Split sentence into words and update word count
    String[] words = sentence.split(" ");
    for (String word : words) {
      word = word.toLowerCase(); // Normalize to lowercase
      wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
    }

    // Emit the word counts (this can be sent to another bolt for further processing, e.g., printing or saving)
    for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
      // Emit a tuple with the word and its count
      emit(new Values(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public void cleanup() {
    // Cleanup any resources after the topology execution ends
    wordCount.clear();
  }
}
