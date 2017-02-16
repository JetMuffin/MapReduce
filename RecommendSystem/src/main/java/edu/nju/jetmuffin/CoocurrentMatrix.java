package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by cj on 2017/2/12.
 */
public class CoocurrentMatrix {
    public static class CoocurrentMatrixMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] splits = value.toString().split("\t");
            String[] comments = splits[1].split(",");
            for(String i: comments) {
                String item1 = i.split(":")[0];
                for(String j: comments) {
                    String item2 = j.split(":")[0];
                    k.set(item1 + ":" + item2);
                    context.write(k, v);
                }
            }
        }
    }

    public static class CoocurrentMatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void run(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "CoocurrentMatrix");
        job.setJarByClass(CoocurrentMatrix.class);
        job.setMapperClass(CoocurrentMatrix.CoocurrentMatrixMapper.class);
        job.setReducerClass(CoocurrentMatrix.CoocurrentMatrixReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setNumReduceTasks(20);
        FileInputFormat.addInputPath(job, new Path(conf.get("step1")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("step2")));
        job.waitForCompletion(true);
    }
}
