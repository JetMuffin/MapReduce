package edu.nju.jetmuffin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by jeff on 16-10-29.
 */
public class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {
    public int getPartition(Text key, IntWritable value, int i) {
        String term = key.toString().split("#")[0];
        return super.getPartition(new Text(term), value, i);
    }
}
