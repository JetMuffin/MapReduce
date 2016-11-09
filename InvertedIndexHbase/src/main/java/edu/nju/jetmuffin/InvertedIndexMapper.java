package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/**
 * Created by jeff on 16-10-29.
 */
public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text term = new Text();
    private Connection connection = null;
    private HTable table = null;
    private Configuration conf = null;
    private Set<String> stopwords = new HashSet<String>();

    public void setup(Context context) throws IOException, InterruptedException{
        conf = context.getConfiguration();
        connection = ConnectionFactory.createConnection(conf);
        table = (HTable)connection.getTable(TableName.valueOf(conf.get("TABLE_STOPWORDS")));

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r: rs) {
            for(Cell cell : r.rawCells()) {
                stopwords.add(new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8"));
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        Map<String, Integer> dict = new HashMap<String, Integer>();

        // Get the name of input file
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String document = fileSplit.getPath().getName();
        document = document.replaceAll(".txt.segmented", "").replaceAll(".TXT.segmented", "");

        // Tokenize
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            // Filter stopwords
            if (stopwords.contains(word)) continue;

            if(dict.containsKey(word)) {
                dict.put(word, dict.get(word) + 1);
            } else {
                dict.put(word, 1);
            }
        }

        // Emit term
        for (Map.Entry<String, Integer> entry : dict.entrySet()) {
            term.set(entry.getKey() + "#" + document);
            context.write(term, new IntWritable(entry.getValue()));
        }
    }
}
