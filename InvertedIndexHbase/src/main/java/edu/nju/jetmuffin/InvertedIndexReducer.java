package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jeff on 16-10-29.
 */
public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Connection connection = null;
    private HTable table = null;
    private Configuration conf = null;
    private StringBuffer postingList = new StringBuffer();
    private int frequent = 0;
    private int count = 0;
    private String lastTerm = null;

    protected void setup(Context context) throws IOException, InterruptedException{
        conf = context.getConfiguration();
        connection = ConnectionFactory.createConnection(conf);
        table = (HTable)connection.getTable(TableName.valueOf(conf.get("TABLE_OUTPUT")));
    }

    public void insert(String row, String cf, String column, String value) throws IOException {
        Put put = new Put(row.getBytes());
        put.addColumn(cf.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: values) {
            sum += val.get();
        }

        String term = key.toString().split("#")[0];
        String document = key.toString().split("#")[1];
        if(lastTerm != null && !term.equals(lastTerm)) {
            double averageFrequent = (double)frequent / (double)count;
            context.write(new Text(lastTerm), new Text(postingList.toString()));
            insert(term, conf.get("COLUMN_FAMILY"), conf.get("COLUMN"), Double.toString(averageFrequent));
            // Reset string buffer
            postingList.setLength(0);
            // Reset frequent count
            frequent = 0;
            // Reset document count
            count = 0;
        }
        postingList.append(document).append(":").append(Integer.toString(sum)).append(" ");
        frequent += sum;
        count += 1;
        lastTerm = term;
    }

    protected void cleanup(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String term = key.toString().split("#")[0];
        double averageFrequent = (double)frequent / (double)count;
        context.write(new Text(lastTerm), new Text(postingList.toString()));
        insert(term, conf.get("COLUMN_FAMILY"), conf.get("COLUMN"), Double.toString(averageFrequent));

        // close connection
        table.close();
        connection.close();
    }
}
