package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cj on 2017/2/12.
 */
public class MatrixMultiply {
    public static class MatrixMultiplyPreMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        private static String flag;

        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");

            if(context.getConfiguration().get("step2").endsWith(flag)) {
                String[] items = splits[0].split(":");
                String number = splits[1];

                k.set(items[0]);
                v.set("A:" + items[1] + "," + number);
                context.write(k, v);
            } else if (context.getConfiguration().get("step1").endsWith(flag)) {
                String userId = splits[0];
                String[] comments = splits[1].split(",");

                for(String comment: comments) {
                    String[] items = comment.split(":");
                    k.set(items[0]);
                    v.set("B:" + userId + "," + items[1]);
                    context.write(k, v);
                }
            }
        }
    }

    public static class MatrixMultiplyPreReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final static Text k = new Text();
        private final static DoubleWritable v = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> itemMap = new HashMap<String, Double>();
            Map<String, Double> userMap = new HashMap<String, Double>();

            for(Text val: values) {
                String[] splits = val.toString().split(":");
                String id = splits[1].split(",")[0];
                Double num = Double.parseDouble(splits[1].split(",")[1]);

                if(splits[0].equals("A")) {
                    itemMap.put(id, num);
                } else if(splits[0].equals("B")) {
                    userMap.put(id, num);
                }
            }

            for(Map.Entry<String, Double> itemEntry : itemMap.entrySet()) {
                for(Map.Entry<String, Double> userEntry : userMap.entrySet()) {
                    double value = itemEntry.getValue() * userEntry.getValue();
                    k.set(userEntry.getKey() + "," + itemEntry.getKey());
                    v.set(value);
                    context.write(k, v);
                }
            }
        }
    }

    public static class MatrixMultiplyCalMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static Text k = new Text();
        private final static DoubleWritable v = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            k.set(splits[0]);
            v.set(Double.parseDouble(splits[1]));
            context.write(k, v);
        }
    }

    public static class MatrixMultiplyCalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final static Text k = new Text();
        private final static DoubleWritable v = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for(DoubleWritable val : values) {
                sum += val.get();
            }
            k.set(key);
            v.set(sum);
            context.write(k, v);
        }
    }

    public static void run(Configuration conf) throws Exception {
        Job preJob = Job.getInstance(conf, "MatrixMultiplyPre");
        preJob.setJarByClass(MatrixMultiply.class);
        preJob.setMapperClass(MatrixMultiply.MatrixMultiplyPreMapper.class);
        preJob.setReducerClass(MatrixMultiply.MatrixMultiplyPreReducer.class);
        preJob.setMapOutputKeyClass(Text.class);
        preJob.setMapOutputValueClass(Text.class);
        preJob.setOutputKeyClass(Text.class);
        preJob.setOutputValueClass(DoubleWritable.class);
//        preJob.setNumReduceTasks(20);
        FileInputFormat.setInputPaths(preJob, new Path(conf.get("step1")), new Path(conf.get("step2")));
        FileOutputFormat.setOutputPath(preJob, new Path(conf.get("step3_1")));
        preJob.waitForCompletion(true);

        Job calJob = Job.getInstance(conf, "MatrixMultiplyCal");
        calJob.setJarByClass(MatrixMultiply.class);
        calJob.setMapperClass(MatrixMultiplyCalMapper.class);
        calJob.setReducerClass(MatrixMultiplyCalReducer.class);
        calJob.setMapOutputKeyClass(Text.class);
        calJob.setMapOutputValueClass(DoubleWritable.class);
        calJob.setOutputKeyClass(Text.class);
        calJob.setOutputValueClass(DoubleWritable.class);
//        calJob.setNumReduceTasks(20);
        FileInputFormat.addInputPath(calJob, new Path(conf.get("step3_1")));
        FileOutputFormat.setOutputPath(calJob, new Path(conf.get("step3_2")));
        calJob.waitForCompletion(true);
    }
}
