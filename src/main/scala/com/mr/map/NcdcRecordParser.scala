package com.mr.map

import org.apache.hadoop.io.Text

class NcdcRecordParser {

  var year = 1900
  var airTemperature = 0
  var quality = ""
  var stationId=""
  

  def parse(record: String): Unit = {

    year = Integer.parseInt(record.substring(15, 19))

    var airTemperatureString: String = ""

    if (record.charAt(87) == '+') {
      airTemperatureString = record.substring(88, 92)
    } else {
      airTemperatureString = record.substring(87, 92)
    }

    airTemperature = Integer.parseInt(airTemperatureString)
    quality = record.substring(92, 93)
    stationId =record.substring(4,10)
    
    

  }
  
  def parse(record: Text):Unit = {
    parse(record.toString())
  }
  
  def isValidTemperature() : Boolean = {
    if(airTemperature != NcdcRecordParser.MISSING_TEMPERATURE && quality.matches("[01459]")) true else false
  }
  
 
}

object NcdcRecordParser {

  val MISSING_TEMPERATURE = 9999

}