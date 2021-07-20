package viettel.spark.sql.log.parquetfile;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetFileHandler {

  public void writeToParquet(List<GenericData.Record> recordList, Schema schema) {
    // Path to Parquet file in HDFS
    Path path = new Path("empRecord.parquet");
    ParquetWriter<GenericData.Record> writer = null;
    // Creating ParquetWriter using builder
    try {
//      writer = AvroParquetWriter
//        .<GenericData.Record>builder(path)
//        .withSchema(schema)
//        .withConf(new Configuration())
//        .withCompressionCodec(CompressionCodecName.SNAPPY)
//        .build();

      for (GenericData.Record record : recordList) {
        writer.write(record);
      }

    }catch(IOException e) {
      e.printStackTrace();
    }
  }

  private List<GenericData.Record> getRecords(Schema schema){
    List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
    GenericData.Record record = new GenericData.Record(schema);
    // Adding 2 records
    record.put("sparkApp", "zzzzzzzzzz");
    record.put("sessionId", "xxxxxxxxx");
    record.put("groupId", "cccccccccccc");
    record.put("ip", "12321312");
    record.put("statement", "vvvvvvvvvvvvv");
    record.put("startTime", "nnnnnnnn");
    recordList.add(record);

    record.put("sparkApp", "aa");
    record.put("sessionId", "bb");
    record.put("groupId", "cc");
    record.put("ip", "666");
    record.put("statement", "dd");
    record.put("startTime", "ee");
    recordList.add(record);

    return recordList;
  }

  public Schema parseSchema() {
    Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema schemaSparkQuery = null;

    try{
      schemaSparkQuery = parser.parse(new FileInputStream("../etc/schema_spark_query.json"));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return schemaSparkQuery;
  }
}
