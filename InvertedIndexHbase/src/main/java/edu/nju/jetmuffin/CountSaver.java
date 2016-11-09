package edu.nju.jetmuffin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jeff on 16-11-7.
 */
public class CountSaver {
    public static void main(String[] args) throws IOException {
        FileWriter writer = new FileWriter("/tmp/wuxia");
        BufferedWriter bw = new BufferedWriter(writer);

        Configuration cfg = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(cfg);
        HTable table = (HTable) connection.getTable(TableName.valueOf("Wuxia".getBytes()));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r: rs) {
            for(Cell cell : r.rawCells()) {
                bw.write(new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8"));
                bw.write("\t");
                bw.write(new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8"));
                bw.write("\n");
            }
        }

        bw.close();
        writer.close();
        table.close();
        connection.close();
    }
}
