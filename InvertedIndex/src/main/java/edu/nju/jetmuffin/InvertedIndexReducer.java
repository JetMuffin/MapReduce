package edu.nju.jetmuffin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jeff on 16-10-29.
 */
public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private StringBuffer postingList = new StringBuffer();
    private int frequent = 0;
    private int count = 0;
    private String lastTerm = null;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: values) {
            sum += val.get();
        }

        String term = key.toString().split("#")[0];
        String document = key.toString().split("#")[1];
        if(lastTerm != null && !term.equals(lastTerm)) {
            double averageFrequent = (double)frequent / (double)count;
            context.write(new Text(lastTerm), new Text(postingList.toString() + "#" + String.format("%.3f", averageFrequent)));
            // Reset string buffer
            postingList.setLength(0);
            // Reset frequent count
            frequent = 0;
            // Reset document count
            count = 0;
        }
        postingList.append(document).append(":").append(Integer.toString(sum)).append(" ");
        frequent += sum;
        count += 1;
        lastTerm = term;
    }

    protected void cleanup(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double averageFrequent = (double)frequent / (double)count;
        context.write(new Text(lastTerm), new Text(postingList.toString() + "#" + String.format("%.3f", averageFrequent)));
    }
}
