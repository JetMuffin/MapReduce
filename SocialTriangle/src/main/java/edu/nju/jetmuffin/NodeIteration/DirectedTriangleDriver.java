package edu.nju.jetmuffin.NodeIteration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by jeff on 16/11/29.
 */
public class DirectedTriangleDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: hadoop jar SocialTriangle.jar edu.nju.jetmuffin.NodeIteration.TriangleDriver <in> <tmp1> <tmp2> <out>");
            System.exit(2);
        }

        // build graph
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(TriangleDriver.class);
        job1.setMapperClass(DirectedGraphBuilder.GraphMapper.class);
        job1.setReducerClass(DirectedGraphBuilder.GraphReducer.class);
        job1.setPartitionerClass(GraphBuilder.GraphPartitioner.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(50);
        FileInputFormat.setMinInputSplitSize(job1, 1);
        FileInputFormat.setMaxInputSplitSize(job1, 5242880);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // build triads
        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(TriangleDriver.class);
        job2.setMapperClass(TriadBuilder.TriadsMapper.class);
//        job2.setCombinerClass(TriadBuilder.TraidsCombiner.class);
        job2.setReducerClass(TriadBuilder.TraidsReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setNumReduceTasks(50);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        // count triangles
        Job job3 = Job.getInstance(conf, "job3");
        job3.setJarByClass(TriangleDriver.class);
        job3.setMapperClass(TriangleCounter.CounterMapper.class);
        job3.setReducerClass(TriangleCounter.CounterReducer.class);
        job3.setCombinerClass(TriangleCounter.CounterReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
