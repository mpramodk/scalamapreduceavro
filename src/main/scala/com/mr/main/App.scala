package com.mr.main

import org.apache.hadoop.util.ToolRunner



/**
 * @author ${user.name}
 */
object App  {
  
 
  
  def main(args : Array[String]) {
  var exitCode = ToolRunner.run(new AvroGenericMaxTemperature() ,args)
  println("exitCode =============> " + exitCode)
  }

}
