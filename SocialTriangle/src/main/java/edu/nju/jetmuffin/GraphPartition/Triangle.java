package edu.nju.jetmuffin.GraphPartition;

import edu.nju.jetmuffin.NodeIteration.GraphBuilder;
import edu.nju.jetmuffin.NodeIteration.TriangleDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by jeff on 16/11/19.
 */
public class Triangle {
    public static class GraphMapper extends Mapper<Object, Text, Text, Text> {
        private int partitionInterval;
        private int partitonNumber;
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        private boolean partitionContains(int i, int a, int b, int c) {
            return i == a || i == b || i == c;
        }

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            partitonNumber = conf.getInt("PARTITION_NUMBER", 0);
            partitionInterval = conf.getInt("PARTITION_INTERVAL", 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split(" ");
            int fromNode = Integer.parseInt(splits[0]);
            int toNode = Integer.parseInt(splits[1]);

            int fromNodePartiton = fromNode / partitionInterval;
            int toNodePartition = toNode / partitionInterval;

            for(int a = 0; a < partitonNumber - 2; a++) {
                for(int b = a + 1; b < partitonNumber - 1; b++) {
                    for(int c = b + 1; c < partitonNumber; c++) {
                        if(partitionContains(fromNodePartiton, a, b, c) && partitionContains(toNodePartition, a, b, c)) {
                            outputKey.set(Integer.toString(a) + " " + Integer.toString(b) + " " + Integer.toString(c));
                            outputValue.set(Integer.toString(fromNode) + " " + Integer.toString(toNode));
                            context.write(outputKey, outputValue);
                        }
                    }
                }
            }
        }
    }

    public static class GraphReducer extends Reducer<Text, Text, Text, IntWritable> {
        private HashMap<String, ArrayList<String>> graph = new HashMap<String, ArrayList<String>>();
        private HashSet<String> edges = new HashSet<String>();
        private ArrayList<String> list = new ArrayList<String>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Partition:" + key.toString());
            int sum = 0;


            for(Text value: values) {
                edges.add(value.toString());

                String[] splits = value.toString().split(" ");
                String fromNode = splits[0];
                String toNode = splits[1];

                if (!graph.containsKey(fromNode)) {
                    list = new ArrayList<String>();
                    list.add(toNode);
                    graph.put(fromNode, list);
                } else {
                    list = graph.get(fromNode);
                    list.add(toNode);
                    graph.put(fromNode, list);
                }
            }

            for (HashMap.Entry<String, ArrayList<String>> entry: graph.entrySet()) {
                String u = entry.getKey();
                System.out.println(u);
                list = entry.getValue();
                for(String v: list) {
                    System.out.print(v + " ");
                }
                System.out.print("\n");
            }

            for (HashMap.Entry<String, ArrayList<String>> entry: graph.entrySet()) {
                String u = entry.getKey();
                list = entry.getValue();
                for (int i = 0; i < list.size() - 1; i++) {
                    for(int j = i + 1; j < list.size(); j++) {
                        String v = list.get(i);
                        String w = list.get(j);
                        System.out.println(v + " " + w);
                        if (list.size() > graph.get(v).size() && graph.get(v).size() > graph.get(w).size()) {
                            if(edges.contains(u + " " + v)) {
                                sum ++;
                            }
                        }
                    }
                }
            }

            context.write(new Text("count"), new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar SocialTriangle.jar edu.nju.jetmuffin.GraphPartition.TriangleDriver <in> <out>");
            System.exit(2);
        }

        conf.set("PARTITION_INTERVAL", "3");
        conf.set("PARTITION_NUMBER", "4");

        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(TriangleDriver.class);
        job1.setMapperClass(Triangle.GraphMapper.class);
        job1.setReducerClass(Triangle.GraphReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
