package edu.nju.jetmuffin.NodeIteration;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;

/**
 * Created by jeff on 16/11/19.
 */
public class GraphBuilder {

    public static class GraphMapper extends Mapper<Object, Text, Text, NullWritable> {
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
                context.write(new Text(toNode + " " + fromNode), NullWritable.get());
            } else {
                context.write(new Text(fromNode + " " + toNode), NullWritable.get());
            }
        }
    }

    public static class GraphPartitioner extends HashPartitioner<Text, NullWritable> {
        public int getPartition(Text key, NullWritable value, int i) {
            // Enforce same keys that has same from node to emit to one machine
            String from = key.toString().split(" ")[0];
            return super.getPartition(new Text(from), value, i);
        }
    }

    public static class GraphReducer extends Reducer<Text, NullWritable, Text, Text> {
        private StringBuffer edges = new StringBuffer();
        private String lastFromNode = null;
        private String lastToNode = null;
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
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

            edges.append(toNode).append(" ");
            lastFromNode = fromNode;
            lastToNode = toNode;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(lastToNode != null) {
                outputKey.set(lastFromNode);
                outputValue.set(edges.toString());
                context.write(outputKey, outputValue);
            }
        }
    }
}
