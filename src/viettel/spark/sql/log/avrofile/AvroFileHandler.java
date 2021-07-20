package viettel.spark.sql.log.avrofile;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import viettel.spark.sql.log.parquetfile.ParquetFileHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class AvroFileHandler {
  public static void main(String[] args) {
    ParquetFileHandler a = new ParquetFileHandler();
    Schema schema = a.parseSchema();
    writeToAvro(schema);
  }

  public static void writeToAvro(Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    // Adding 2 records
    record.put("sparkApp", "zzzzzzzzzz");
    record.put("sessionId", "xxxxxxxxx");
    record.put("groupId", "cccccccccccc");
    record.put("ip", "12321312");
    record.put("statement", "vvvvvvvvvvvvv");
    record.put("startTime", "nnnnnnnn");

    GenericRecord person2 = new GenericData.Record(schema);
    person2.put("sparkApp", "aa");
    person2.put("sessionId", "bb");
    person2.put("groupId", "cc");
    person2.put("ip", "666");
    person2.put("statement", "dd");
    person2.put("startTime", "ee");

//    GenericRecord person2 = new GenericData.Record(schema);
//    person2.put("id", 2);
//    person2.put("Name", "Jill");
//    person2.put("Address", "2, Richmond Drive");

    DatumWriter<GenericRecord> datumWriter = new
      GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = null;
    try {
      //out file path in HDFS
      Configuration conf = new Configuration();
      // change the IP
      FileSystem fs = FileSystem.get(URI.create(
        "person.avro"), conf);
      OutputStream out = fs.create(new Path(
        "person.avro"));

      dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
      // for compression
      //dataFileWriter.setCodec(CodecFactory.snappyCodec());
      dataFileWriter.create(schema, out);

      dataFileWriter.append(record);
      dataFileWriter.append(person2);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (dataFileWriter != null) {
        try {
          dataFileWriter.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
}
