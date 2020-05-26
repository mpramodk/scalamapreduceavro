package com.mr.main

import org.apache.avro.Schema
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool

import com.mr.map.MaxTemperatureMapper
import com.mr.schema.AvroSchema
import org.apache.avro.generic.GenericRecord
import com.mr.reduce.MaxTemperatureReducer


class AvroGenericMaxTemperature extends Configured with Tool {

  def run(args: Array[String]): Int = {
    var result = 0
    if (args.length < 2) {
      println("Not enough arguments were passed")
      return -1
    }

    var job = new Job(getConf(), "Max Temperature")
    job.setJarByClass(classOf[AvroGenericMaxTemperature])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT))
    AvroJob.setMapOutputValueSchema(job, AvroSchema.SCHEMA)
    AvroJob.setOutputKeySchema(job, AvroSchema.SCHEMA)

    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[AvroKeyOutputFormat[GenericRecord]])
    
    job.setMapperClass(classOf[MaxTemperatureMapper])
    job.setReducerClass(classOf[MaxTemperatureReducer])
    
    println("From Configuration ====> " + getConf().toString())
    
    
    result = if (job.waitForCompletion(true)) 0 else 1
    result
   }

}