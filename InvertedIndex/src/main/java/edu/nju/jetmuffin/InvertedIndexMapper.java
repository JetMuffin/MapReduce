package edu.nju.jetmuffin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by jeff on 16-10-29.
 */
public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text term = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        Map<String, Integer> dict = new HashMap<String, Integer>();

        // Get the name of input file
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String document = fileSplit.getPath().getName();
        document = document.replaceAll(".txt.segmented", "").replaceAll(".TXT.segmented", "");

        // Tokenize
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            if(dict.containsKey(word)) {
                dict.put(word, dict.get(word) + 1);
            } else {
                dict.put(word, 1);
            }
        }

        // Emit term
        for (Map.Entry<String, Integer> entry : dict.entrySet()) {
            term.set(entry.getKey() + "#" + document);
            context.write(term, new IntWritable(entry.getValue()));
        }
    }
}
