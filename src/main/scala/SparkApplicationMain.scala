package com.kdg.spark

import au.com.eliiza.urbanforest.{Loop, MultiPolygon, intersectionArea, mayIntersect, mergeMultiPolygons, multiPolygonArea}
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import java.util.regex.Pattern


case class Geometry(coordinates: MultiPolygon)
case class Suburb(sa3_code16:String,  sa3_name16:String, geometry:Geometry)
case class TerritoryCoordinates(id:String, coordinates:MultiPolygon)
case class TerritoryArea(id:String, coordinates:MultiPolygon, area:Double)
case class TerritoryForestIntersectArea(id:String, area:Double, intersect: Double)
case class TerritoryForestRatio(id:String, ratio:Double)

object SparkApplicationMain {

  def main(args: Array[String]): Unit = {

    // Spark Session
    val spark: SparkSession = SparkSession.builder()
      .appName("TerritoryForestRatio")
      .master("local[*]")
      .config("spark.executor.instances", "2")
      .config("spark.executor.memory", "5g")
      .config("spark.driver.memory", "8g")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","4g")
      .config("spark.network.timeout","1800s")
      .getOrCreate()

    // Get Coordinates of Forest
    val forestCoordinatesMP = spark.sparkContext.broadcast(getForestCoordinates(spark))

    // Get Coordinates of Territories
    val territoryCoordinatesDS = getTerritoryCoordinatesDS(spark)

    // Get Territory Intersect Area Ratio
    import spark.sqlContext.implicits._

    val territoryIntersectRatioDS = territoryCoordinatesDS
      .filter(territory => mayIntersect(territory.coordinates, forestCoordinatesMP.value))
      .map(territory => TerritoryArea(territory.id, territory.coordinates, multiPolygonArea(territory.coordinates)))
      .map(territory => TerritoryForestIntersectArea(territory.id, territory.area, intersectionArea(territory.coordinates, forestCoordinatesMP.value)))
      .map(territory => TerritoryForestRatio(territory.id, territory.intersect/territory.area))
      .reduce((t1, t2) => if (t1.ratio > t2.ratio) t1 else t2)
      //--.sort($"ratio".desc)
      //--.first()

    // Show Territory which have highest Ratio
    println("Highest Ratio have " + territoryIntersectRatioDS.id + " and it is " + territoryIntersectRatioDS.ratio)

    // Stop Spark Session
    spark.stop()
  }

  def getForestCoordinates(spark: SparkSession): MultiPolygon = {
    import spark.sqlContext.implicits._

    val path = "data/melb_urban_forest_2016.txt"
    val textDS = spark.read.textFile(path)

    val polygonsDS = textDS.map(polygonStr => {
      val pattern = Pattern.compile("\\(([^)]+)\\)")
      val matcher = pattern.matcher(polygonStr)

      var loops = Seq[Loop]()
      while(matcher.find()) {
        val loopLine = matcher.group(1)
        val loopStr = if (loopLine.contains("(")) loopLine.substring(1) else loopLine
        val loop: Loop = loopStr
          .split(" ")
          .map(value => value.toDouble)
          .grouped(2)
          .map(x => Seq(x.head, x.tail.head))
          .toSeq

        loops = loops :+ loop
      }

      loops
    })

    polygonsDS.collect().toSeq
  }

  def getTerritoryCoordinatesDS(spark: SparkSession): Dataset[TerritoryCoordinates] = {
    import spark.sqlContext.implicits._

    val path = "data/melb_inner_2016.json"
    val suburbDS = spark.read.json(path).as[Suburb]

    val suburbCoordinatesDS = suburbDS.select("sa2_name16", "geometry.coordinates")
    val territorySuburbsCoordinatesDS = suburbCoordinatesDS.groupBy("sa2_name16").agg(functions.collect_list("coordinates").as("coordinates"))
    val territoryCoordinatesDS = territorySuburbsCoordinatesDS.map(row => TerritoryCoordinates(row.getString(0), mergeMultiPolygons(row.getSeq[MultiPolygon](1).flatten)))

    territoryCoordinatesDS
  }
}
