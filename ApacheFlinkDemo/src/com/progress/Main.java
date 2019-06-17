package com.progress;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.table.api.java.BatchTableEnvironment;


public class Main {

    public static class RowFlat implements FlatMapFunction<Tuple2<Row, Row>, Row> {
        @Override
        public void flatMap(Tuple2<Row, Row> value, Collector<Row> out) {

            Row row = new Row(3);
            row.setField(0, value.f0.getField(0));
            row.setField(1, value.f0.getField(1));
            row.setField(2, value.f1.getField(1));
            out.collect(row);
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.ddtek.jdbc.sqlserver.SQLServerDriver")
                .setDBUrl("jdbc:datadirect:sqlserver://localhost:1433;databaseName=Chinook")
                .setUsername("username")
                .setPassword("password")
                .setQuery("SELECT [AlbumId], [Title] ,[ArtistId] FROM [Chinook].[dbo].[Album]")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        TypeInformation<?>[] fieldTypes2 = new TypeInformation<?>[] {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO
                 };
        RowTypeInfo rowTypeInfo2 = new RowTypeInfo(fieldTypes2);
        JDBCInputFormat jdbcInputFormat2 = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.ddtek.jdbc.sqlserver.SQLServerDriver")
                .setDBUrl("jdbc:datadirect:sqlserver://localhost:1433;databaseName=Chinook;")
                .setUsername("username")
                .setPassword("password")
                .setQuery("SELECT [ArtistId], [Name]  FROM [Chinook].[dbo].[Artist]")
                .setRowTypeInfo(rowTypeInfo2)
                .finish();

        DataSet<Row> albumData = env.createInput(jdbcInputFormat);
        DataSet<Row> artistData = env.createInput(jdbcInputFormat2);

        DataSet<Tuple2<Row, Row>> joinedData = albumData.join(artistData).where("f2").equalTo("f0");
        DataSet<Row> rowDataSet = joinedData.flatMap(new RowFlat());

        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.ddtek.jdbc.hive.HiveDriver")
                .setDBUrl("jdbc:datadirect:hive://hiveserver:10000;")
                .setUsername("username")
                .setPassword("password")
                .setQuery("INSERT INTO albums (albumid, name, artistname) VALUES (?, ?, ?)")
                .finish();

        rowDataSet.output(jdbcOutput);
        env.execute();
    }

}
