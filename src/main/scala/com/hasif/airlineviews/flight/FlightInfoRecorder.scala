package com.hasif.airlineviews.flight

import java.util.Calendar

import com.hasif.airlineviews.AirlineViewDriver
import com.hasif.airlineviews.constants.AirlineConstants
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions.{count, regexp_extract}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}

class FlightInfoRecorder {

  val spark = AirlineViewDriver.spark

  import spark.implicits._

  spark.conf.set("spark.sql.shuffle.partitions", "5")


  val csv = spark.read.format("csv").option("header", true)

  def ingestFlightData(filePath: String) = {
    val startTime = Calendar.getInstance()

    val airlineSchema = StructType(Array(StructField("IATA_CODE", StringType, true),
      StructField("AIRLINE", StringType, true)))
    val airlines = csv.schema(airlineSchema).load(s"$filePath/${AirlineConstants.AIRLINES_FILE}")

    val flightSchema = StructType(Array(StructField("YEAR", IntegerType, true),
      StructField("MONTH", IntegerType, true),
      StructField("DAY", IntegerType, true),
      StructField("DAY_OF_WEEK", IntegerType, true),
      StructField("AIRLINE", StringType, true),
      StructField("FLIGHT_NUMBER", IntegerType, true),
      StructField("TAIL_NUMBER", StringType, true),
      StructField("ORIGIN_AIRPORT", StringType, true),
      StructField("DESTINATION_AIRPORT", StringType, true),
      StructField("SCHEDULED_DEPARTURE", IntegerType, true),
      StructField("DEPARTURE_TIME", IntegerType, true),
      StructField("DEPARTURE_DELAY", IntegerType, true),
      StructField("TAXI_OUT", IntegerType, true),
      StructField("WHEELS_OFF", IntegerType, true),
      StructField("SCHEDULED_TIME", IntegerType, true),
      StructField("ELAPSED_TIME", IntegerType, true),
      StructField("AIR_TIME", IntegerType, true),
      StructField("DISTANCE", IntegerType, true),
      StructField("WHEELS_ON", IntegerType, true),
      StructField("TAXI_IN", IntegerType, true),
      StructField("SCHEDULED_ARRIVAL", IntegerType, true),
      StructField("ARRIVAL_TIME", IntegerType, true),
      StructField("ARRIVAL_DELAY", IntegerType, true),
      StructField("DIVERTED", IntegerType, true),
      StructField("CANCELLED", IntegerType, true),
      StructField("CANCELLATION_REASON", StringType, true),
      StructField("AIR_SYSTEM_DELAY", IntegerType, true),
      StructField("SECURITY_DELAY", IntegerType, true),
      StructField("AIRLINE_DELAY", IntegerType, true),
      StructField("LATE_AIRCRAFT_DELAY", IntegerType, true),
      StructField("WEATHER_DELAY", IntegerType, true)))
    val flights = csv.schema(flightSchema).load(s"$filePath/${AirlineConstants.FLIGHTS_FILE}")

    val flightAirlineDetails = findAirlineDetails(airlines, flights)


    val airportSchema = StructType(Array(StructField("IATA_CODE", StringType, true),
      StructField("AIRPORT", StringType, true),
      StructField("CITY", StringType, true),
      StructField("STATE", StringType, true),
      StructField("COUNTRY", StringType, true),
      StructField("LATITUDE", DoubleType, true),
      StructField("LONGITUDE", DoubleType, true)))
    val airports = csv.schema(airportSchema).load(s"$filePath/${AirlineConstants.AIRPORTS_FILE}")

    val validFlightAirportDetails = findAirportDetails(airports, flightAirlineDetails, "inner")
    validFlightAirportDetails.write.parquet("hdfs://localhost:9000/valid/airline/2015/")

    val invalidFlightAirportDetails = findAirportDetails(airports, flightAirlineDetails, "left_anti")
    invalidFlightAirportDetails.write.csv("hdfs://localhost:9000/invalid/airline/2015/")

    validFlightAirportDetails.show(5, false)
    val flightCount = flights.count
    val validFlightAirline = flightAirlineDetails.count()
    val validFlightAirport = validFlightAirportDetails.count
    val invalidFlightAirport = invalidFlightAirportDetails.count
    println(s"Number of flight records : $flightCount")
    println(s"Number of flight with valid airlines : $validFlightAirline")
    println(s"Number of valid flight airport mapping : $validFlightAirport")
    println(s"Number of invalid flight airport mapping : $invalidFlightAirport")
    println(s"Difference : ${validFlightAirline - validFlightAirport - invalidFlightAirport}")

    val endTime = Calendar.getInstance()
    println(s"Time taken for processing : ${endTime.getTimeInMillis - startTime.getTimeInMillis}ms")
  }

  def findAirportDetails(airports: DataFrame, flights: DataFrame, joinType: String) = {
    val origin = airports.withColumnRenamed("IATA_CODE", "ORIGIN_IATA_CODE")
      .withColumnRenamed("AIRPORT", "ORIGIN_AIRPORT_NAME")
      .withColumnRenamed("CITY", "ORIGIN_CITY")
      .withColumnRenamed("STATE", "ORIGIN_STATE")
      .withColumnRenamed("COUNTRY", "ORIGIN_COUNTRY")
      .withColumnRenamed("LATITUDE", "ORIGIN_LATITUDE")
      .withColumnRenamed("LONGITUDE", "ORIGIN_LONGITUDE")
    val destination = airports.withColumnRenamed("IATA_CODE", "DESTINATION_IATA_CODE")
      .withColumnRenamed("AIRPORT", "DESTINATION_AIRPORT_NAME")
      .withColumnRenamed("CITY", "DESTINATION_CITY")
      .withColumnRenamed("STATE", "DESTINATION_STATE")
      .withColumnRenamed("COUNTRY", "DESTINATION_COUNTRY")
      .withColumnRenamed("LATITUDE", "DESTINATION_LATITUDE")
      .withColumnRenamed("LONGITUDE", "DESTINATION_LONGITUDE")
    val originExpression = origin.col("ORIGIN_IATA_CODE") === flights.col("ORIGIN_AIRPORT")
    val destinationExpression = destination.col("DESTINATION_IATA_CODE") === flights.col("DESTINATION_AIRPORT")

    flights.join(origin, originExpression, joinType)
      .join(destination, destinationExpression, joinType).drop("ORIGIN_IATA_CODE", "DESTINATION_IATA_CODE")
  }

  def findAirlineDetails(airlines: DataFrame, flights: DataFrame) = {
    val airline = airlines.withColumnRenamed("AIRLINE", "AIRLINE_NAME")
    val airlineExpression = airline.col("IATA_CODE") === flights.col("AIRLINE")
    flights.join(airline, airlineExpression).drop("IATA_CODE")
  }
}
