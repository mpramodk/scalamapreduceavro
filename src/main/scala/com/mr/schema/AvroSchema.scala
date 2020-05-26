package com.mr.schema

import org.apache.avro.Schema

object AvroSchema {
  
  val SCHEMA = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
  
  
}