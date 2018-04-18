package com.hasif.airlineviews

import com.hasif.airlineviews.flight.FlightInfoRecorder
import org.apache.spark.sql.SparkSession

object AirlineViewDriver {

  val spark = SparkSession.builder().master("local").appName("Airline Views").getOrCreate()
  def main(args: Array[String]): Unit = {
    if(args.size == 1) {
      val loadFlightData = new FlightInfoRecorder
      val flightDataPath = args(0)
        loadFlightData.ingestFlightData(flightDataPath)
    }
  }
}
