package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class UserPreference {

    public static class UserPreferenceMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] splits = value.toString().split(",");
            String userId = splits[0];
            String movieId = splits[1];
            String score = splits[2];

            k.set(userId);
            v.set(movieId + ":" + score);
            context.write(k, v);
        }
    }

    public static class UserPreferenceReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text v = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();

            for(Text val: values) {
                sb.append(",").append(val.toString());
            }
            v.set(sb.toString().replaceFirst(",", ""));

            context.write(key, v);
        }
    }

    public static void run(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "UserPreference");
        job.setJarByClass(UserPreference.class);
        job.setMapperClass(UserPreferenceMapper.class);
        job.setReducerClass(UserPreferenceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(20);
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("step1")));
        job.waitForCompletion(true);
    }
}
