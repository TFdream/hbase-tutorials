package com.mindflow.hbase.tutorials.crud;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * http://hbase.apache.org/1.2/apidocs/index.html
 *
 * @author Ricky Fung
 */
public class HBaseCrudDemo {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args) {

        new HBaseCrudDemo().testCrud();
    }

    public void testCrud() {
        Connection connection = null;
        try {
            connection = HBaseConnectionUtils.getConnection();
            TableName tableName = TableName.valueOf("demo");

            //创建HBase表
            createTable(connection, tableName, "cf1", "cf2");

            //put
            String rowKey = "u12000";
            put(connection, tableName, rowKey, "cf1", "name", "ricky");
            put(connection, tableName, rowKey, "cf1", "password", "root");
            put(connection, tableName, rowKey, "cf1", "age", "28");

            //get
            get(connection, tableName, rowKey);

            //scan
            scan(connection, tableName);

            //delete
            deleteTable(connection, tableName);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void scan(Connection connection, TableName tableName) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            ResultScanner rs = null;
            try {
                //Scan scan = new Scan(Bytes.toBytes("u120000"), Bytes.toBytes("u200000"));
                rs = table.getScanner(new Scan());
                for(Result r:rs){
                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
                    for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()){
                        logger.info("row:{} key:{}", Bytes.toString(r.getRow()), Bytes.toString(entry.getKey()));
                        NavigableMap<byte[], NavigableMap<Long, byte[]>> map =entry.getValue();
                        for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en:map.entrySet()){
                            System.out.print(Bytes.toString(en.getKey())+"##");
                            NavigableMap<Long, byte[]> ma = en.getValue();
                            for(Map.Entry<Long, byte[]>e: ma.entrySet()){
                                System.out.print(e.getKey()+"###");
                                System.out.println(Bytes.toString(e.getValue()));
                            }
                        }
                    }
                }
            } finally {
                if(rs!=null) {
                    rs.close();
                }
            }
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    //根据row key获取表中的该行数据
    public void get(Connection connection,TableName tableName,String rowKey) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()){

                logger.info("columnFamily:{}", Bytes.toString(entry.getKey()));
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map =entry.getValue();
                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en:map.entrySet()){
                    System.out.print(Bytes.toString(en.getKey())+"##");
                    NavigableMap<Long, byte[]> nm = en.getValue();
                    for(Map.Entry<Long, byte[]> me : nm.entrySet()){
                        logger.info("column key:{}, value:{}", me.getKey(), me.getValue());
                    }
                }
            }
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    /**批量插入可以使用 Table.put(List<Put> list)**/
    public void put(Connection connection, TableName tableName,
                    String rowKey, String columnFamily, String column, String data) throws IOException {

        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
        } finally {
            if(table!=null) {
                table.close();
            }
        }
    }

    public void createTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if(admin.tableExists(tableName)){
                logger.warn("table:{} exists!", tableName.getName());
            }else{
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for(String columnFamily : columnFamilies) {
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(tableDescriptor);
                logger.info("create table:{} success!", tableName.getName());
            }
        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }

    //删除表中的数据
    public void deleteTable(Connection connection, TableName tableName) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
                //必须先disable, 再delete
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }

    public void disableTable(Connection connection, TableName tableName) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if(admin.tableExists(tableName)){
                admin.disableTable(tableName);
            }
        } finally {
            if(admin!=null) {
                admin.close();
            }
        }
    }
}
