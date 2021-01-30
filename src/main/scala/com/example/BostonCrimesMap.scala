package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BostonCrimesMap extends App {
  if (args.length < 3) {
    println("enter input file name with path, dictionary file name with path and path to output file")
    sys.exit(1)
  }
  val inputFileName = args(0)
  val offenseCodesFileName = args(1)
  val outputFileName = args(2)

  val spark = SparkSession.builder()
    .appName("BostonCrimesStat")
    .getOrCreate()

  import spark.implicits._

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inputFileName)

  val offenseCodes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(offenseCodesFileName)

  val offenseCodesBroadcast = broadcast(offenseCodes).groupBy($"CODE").agg(first($"NAME").as("offense_name")) // remove offense codes duplicates

  val crimesOffenseTop3 = crimeFacts
    .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
    .distinct()
    .groupBy($"DISTRICT", split($"offense_name", "-").getItem(0).as("first_name"))
    .agg(count($"offense_name").as("offense_name_count"))
    .orderBy($"DISTRICT".asc_nulls_last, $"offense_name_count".desc_nulls_last)
    .groupBy($"DISTRICT")
    .agg(slice(collect_list($"first_name"), 1, 3).as("offense_name_arr"))

  val crimesOffenseTop3WithBroadcast = broadcast(crimesOffenseTop3)

  val crimesStatsWithBroadcast = crimeFacts
    .join(crimesOffenseTop3WithBroadcast, crimeFacts("DISTRICT") === crimesOffenseTop3WithBroadcast("DISTRICT"))
    .groupBy(crimeFacts("DISTRICT"), $"YEAR", $"MONTH")
    .agg(count(crimeFacts("DISTRICT")).as("total_count"), sum($"Lat").as("lat_sum"), count($"Lat").as("lat_count"), sum($"Long").as("long_sum"), count($"Long").as("long_count"), first(crimesOffenseTop3WithBroadcast("offense_name_arr")).as("offense_top3_name"))
    .orderBy(crimeFacts("DISTRICT").asc_nulls_last, $"YEAR".asc_nulls_last, $"MONTH".asc_nulls_last)
    .groupBy(crimeFacts("DISTRICT"))
    .agg(sum("total_count").as("crimes_total"), expr("percentile_approx(total_count, 0.5)").as("crimes_monthly"), concat_ws(", ",first("offense_top3_name")).as("frequent_crime_types"), (sum("lat_sum") / sum("lat_count")).as("lat"), (sum("long_sum") / sum("long_count")).as("lng"))
    .orderBy(crimeFacts("DISTRICT").asc_nulls_last)

//  crimesStatsWithBroadcast.show(50, false)
  crimesStatsWithBroadcast.toDF().repartition(1).write.parquet(outputFileName)
}
