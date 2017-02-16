package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by cj on 2017/2/13.
 */
public class ScoreRanking {
    public static class ScoreRankingMapper extends Mapper<Object, Text, Text, Text> {
        private final static Text k = new Text();
        private final static Text v = new Text();
        private static String flag;

        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");

            if(context.getConfiguration().get("step1").endsWith(flag)) {
                String[] comments = splits[1].split(",");
                String userId = splits[0];

                for(String i: comments) {
                    String itemId = i.split(":")[0];
                    k.set(userId);
                    v.set(itemId + " " + "0");
                    context.write(k, v);
                }
            } else if(context.getConfiguration().get("step3_2").endsWith(flag)) {
                String userId = splits[0].split(",")[0];
                String itemId = splits[0].split(",")[1];

                k.set(userId);
                v.set(itemId + " " + splits[1]);
                context.write(k, v);
            }
        }
    }

    public static class ScoreRankingReducer extends Reducer<Text, Text, Text, Text> {
        private static int topK;
        private static Text k = new Text();
        private static Text v = new Text();

        protected void setup(Context context) {
            topK = context.getConfiguration().getInt("topk", 3);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> filterSet = new HashSet<String>();
            Map<String, Double> scoreMap = new HashMap<String, Double>();

            for(Text val: values) {
                String[] splits = val.toString().split(" ");
                String itemId = splits[0];
                double score = Double.parseDouble(splits[1]);

                if(score == 0) {
                    filterSet.add(itemId);
                } else {
                    scoreMap.put(itemId, score);
                }
            }

            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(scoreMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return (o2.getValue() > o1.getValue()) ? 1 : 0;
                }
            });

            int count = 1;
            for(Map.Entry<String, Double> entry : list) {
                if(filterSet.contains(entry.getKey())) {
                    continue;
                } else {
                    k.set(key.toString());
                    v.set(entry.getKey() + "," + entry.getValue());
                    context.write(k, v);

                    count++;
                    if(count > topK) {
                        break;
                    }
                }
            }
        }
    }

    public static void run(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "ScoreRanking");
        job.setJarByClass(ScoreRanking.class);
        job.setMapperClass(ScoreRankingMapper.class);
        job.setReducerClass(ScoreRankingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setNumReduceTasks(20);
        FileInputFormat.setInputPaths(job, new Path(conf.get("step1")), new Path(conf.get("step3_2")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
