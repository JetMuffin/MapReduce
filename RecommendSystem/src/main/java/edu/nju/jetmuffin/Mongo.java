package edu.nju.jetmuffin;

import com.mongodb.hadoop.MongoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.BSONObject;

import java.io.IOException;

/**
 * Created by jeff on 17/2/7.
 */
public class Mongo {
    public static class MongoMapper extends Mapper<Object, BSONObject, Text, Text> {
        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException{
            System.out.println(value.get("name"));
        }
    }

    public static class MongoReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mongo.input.uri", "mongodb://localhost:27017/douban.movies");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: hadoop jar Mongo.jar edu.nju.jetmuffin.Mongo <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "mongo");
        job.setJarByClass(Mongo.class);
        job.setMapperClass(MongoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MongoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(MongoInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
