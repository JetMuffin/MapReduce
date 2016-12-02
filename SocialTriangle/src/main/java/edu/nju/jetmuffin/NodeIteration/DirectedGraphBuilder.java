package edu.nju.jetmuffin.NodeIteration;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jeff on 16/11/29.
 */
public class DirectedGraphBuilder {
    public static class GraphMapper extends Mapper<Object, Text, Text, Text> {
        private Text left = new Text("l");
        private Text right = new Text("r");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            String fromNode = splits[0];
            String toNode = splits[1];

            // Ignore self circle
            if(fromNode.equals(toNode)) {
                return;
            }

            // Convert directed graph to undirected graph.
            // All edges are projected to the lexicographically smaller node of both incident nodes.
            if(fromNode.compareTo(toNode) > 0) {
                context.write(new Text(toNode + " " + fromNode), left);
            } else {
                context.write(new Text(fromNode + " " + toNode), right);
            }
        }
    }

    public static class GraphReducer extends Reducer<Text, Text, Text, Text> {
        private StringBuffer edges = new StringBuffer();
        private String lastFromNode = null;
        private String lastToNode = null;
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] splits = key.toString().split(" ");
            String fromNode = splits[0];
            String toNode = splits[1];

            if(lastFromNode != null && !fromNode.equals(lastFromNode)) {
                outputKey.set(lastFromNode);
                outputValue.set(edges.toString());
                context.write(outputKey, outputValue);
                edges = new StringBuffer();
            }

            // Filter out reduplicate edges
            if(lastToNode != null && fromNode.equals(lastFromNode) && toNode.equals(lastToNode)) {
                return;
            }

            boolean left = false;
            boolean right = false;

            for(Text value: values) {
                if(value.toString().equals("l")) left = true;
                if(value.toString().equals("r")) right = true;
            }
            if(left && right) {
                edges.append(toNode).append(" ");
                lastFromNode = fromNode;
                lastToNode = toNode;
            }
        }
    }
}
