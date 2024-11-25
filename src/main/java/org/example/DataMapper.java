package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<Object, Text, Text, Text> {
    private final Text keyOutput = new Text();
    private final Text valueOutput = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Clé : identifiant ligne, Valeur : contenu de la ligne
        keyOutput.set(key.toString());
        valueOutput.set(value.toString());
        context.write(keyOutput, valueOutput);
    }
}