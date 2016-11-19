package edu.nju.jetmuffin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jeff on 16/11/19.
 */
public class TriadBuilder {
    public static class TriadsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String fromNode = splits[0];
            String[] toNodes = splits[1].split(" ");

            // Edges that are projected to the same node are combined to a traid.
            // Emit all possible candidates with format <(v1,v2), u>.
            for(int i = 0; i < toNodes.length - 1; i++) {
                for(int j = i + 1; j < toNodes.length; j++) {
                    context.write(new Text(toNodes[i] + " " + toNodes[j]), new Text(fromNode));
                }
            }

            // Emit edges from origin graph.
            for(String toNode: toNodes) {
                context.write(new Text(fromNode + " " + toNode), new Text("$"));
            }
        }
    }

    public static class TraidsReducer extends Reducer<Text, Text, IntWritable, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean match = false;
            int count = 0;

            // Check if this traid edge matches edge in origin graph.
            for (Text value: values) {
                count ++;
                if (value.toString().equals("$")) {
                    match = true;
                    count --;
                }
            }

            if (match) {
                context.write(new IntWritable(count), NullWritable.get());
            }
        }
    }
}
