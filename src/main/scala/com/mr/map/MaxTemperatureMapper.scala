package com.mr.map

import org.apache.hadoop.mapreduce.Mapper
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import com.mr.schema.AvroSchema
import java.io.IOException

class MaxTemperatureMapper extends Mapper[LongWritable, Text, AvroKey[Integer], AvroValue[GenericRecord]] {

  val parser = new NcdcRecordParser()
  val record = new GenericData.Record(AvroSchema.SCHEMA)

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def map(key: LongWritable, value: Text, context:Mapper[LongWritable, Text, AvroKey[Integer], AvroValue[GenericRecord]]#Context) = {
    parser.parse(value.toString())
    if (parser.isValidTemperature()) {
      record.put("year", parser.year)
      record.put("temperature", parser.airTemperature)
      record.put("stationId", parser.stationId)
      context.write(new AvroKey[Integer](parser.year), new AvroValue[GenericRecord](record))
    }

  }

}