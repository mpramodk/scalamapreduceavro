package com.mr.reduce

import java.io.IOException

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Reducer
import com.mr.schema.AvroSchema
import org.apache.avro.generic.GenericData
import scala.collection.JavaConverters._

class MaxTemperatureReducer extends  Reducer[AvroKey[Integer],AvroValue[GenericRecord],AvroKey[GenericRecord],NullWritable]{
  
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def reduce(key:AvroKey[Integer],values:java.lang.Iterable[AvroValue[GenericRecord]],
      context:Reducer[AvroKey[Integer],AvroValue[GenericRecord],AvroKey[GenericRecord],NullWritable]#Context) = {
  
    var max: GenericRecord = null
    
    values.asScala.map {value =>
      val record = value.datum()
      val recordTemp = Integer.parseInt(record.get("temperature").toString())
      val maxTemp = if (max != null) Integer.parseInt(max.get("temperature").toString()) else Integer.parseInt("0")
      
      if(max == null || (recordTemp > maxTemp)) {
        max = newWeatherRecord(record) 
      }
        
      
    }
    context.write(new AvroKey(max),NullWritable.get())
  
  }
  
  
  def newWeatherRecord(value: GenericRecord) : GenericRecord = {
    val record = new GenericData.Record(AvroSchema.SCHEMA)
    record.put("year",value.get("year"))
    record.put("temperature",value.get("temperature"))
    record.put("stationId",value.get("stationId"))
    record  
  }
  
  
  
  
  
  
}