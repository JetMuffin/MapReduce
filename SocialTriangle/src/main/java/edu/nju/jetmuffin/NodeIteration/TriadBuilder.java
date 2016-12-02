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
public class TriadBuilder {
    public static class TriadsMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String fromNode = splits[0];
            String[] toNodes = splits[1].split(" ");

            // Edges that are projected to the same node are combined to a traid.
            // Emit all possible candidates with format <(v1,v2), u>.
            for(int i = 0; i < toNodes.length - 1; i++) {
                for(int j = i + 1; j < toNodes.length; j++) {
                    outputKey.set(toNodes[i] + " " + toNodes[j]);
                    outputValue.set(fromNode);
                    context.write(outputKey, outputValue);
                }
            }

            // Emit edges from origin graph.
            for(String toNode: toNodes) {
                outputKey.set(fromNode + " " + toNode);
                outputValue.set("$");
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class TraidsCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer toNodeList = new StringBuffer();
            for(Text value: values) {
                toNodeList.append(value.toString()).append(" ");
            }
            outputValue.set(toNodeList.toString());
            context.write(key, outputValue);
        }
    }

    public static class TraidsReducer extends Reducer<Text, Text, IntWritable, NullWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean match = false;
            int count = 0;
            String[] toNodeList;

            // Check if this traid edge matches edge in origin graph.
            for (Text value: values) {
                toNodeList = value.toString().split(" ");
                for(int i = 0; i < toNodeList.length; i++) {
                    if(toNodeList[i].equals("$")) {
                        match = true;
                    }
                }
                count += toNodeList.length;
            }

            if (match && count > 1) {
                context.write(new IntWritable(count - 1), NullWritable.get());
            }
        }
    }
}
