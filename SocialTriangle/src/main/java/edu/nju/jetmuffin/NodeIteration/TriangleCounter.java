package edu.nju.jetmuffin.NodeIteration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jeff on 16/11/19.
 */
public class TriangleCounter {
    public static class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text countKey = new Text("count");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(countKey, new IntWritable(Integer.parseInt(value.toString())));
        }
    }

    public static class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
