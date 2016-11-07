package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by jeff on 16-10-29.
 */
public class InvertedIndexOutputFormat extends TextOutputFormat<Text, Text> {
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = "\t";
        CompressionCodec codec = null;
        String extension = "";
        if(isCompressed) {
            Class file = getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(file, conf);
            extension = codec.getDefaultExtension();
        }

        Path file1 = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file1.getFileSystem(conf);
        FSDataOutputStream fileOut;
        if(!isCompressed) {
            fileOut = fs.create(file1, false);
            return new InvertedIndexRecordWriter(fileOut, keyValueSeparator);
        } else {
            fileOut = fs.create(file1, false);
            return new InvertedIndexRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }
    }

    protected static class InvertedIndexRecordWriter extends RecordWriter<Text, Text> {
        private final DataOutputStream out;
        private String keyValueSeparator;
        private String newline = "\n";

        public InvertedIndexRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            this.keyValueSeparator = keyValueSeparator;
        }

        private void writeObject(Object o) throws IOException {
            if(o instanceof Text) {
                Text to = (Text)o;
                this.out.write(to.getBytes(), 0, to.getLength());
            } else {
                this.out.write(o.toString().getBytes("UTF-8"));
            }
        }

        public void write(Text key, Text value) throws IOException, InterruptedException {
            String index = value.toString().split("#")[0];
            String averageFrequent = value.toString().split("#")[1];

            // write inverted index
            this.writeObject(key);
            this.writeObject(keyValueSeparator);
            this.writeObject(index);
            this.writeObject(newline);

            // write average frequent
            this.writeObject(key);
            this.writeObject(keyValueSeparator);
            this.writeObject(averageFrequent);
            this.writeObject(newline);
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.out.close();
        }
    }
}