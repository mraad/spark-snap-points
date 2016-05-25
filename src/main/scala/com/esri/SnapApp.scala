package com.esri

import com.databricks.spark.avro._
import com.databricks.spark.csv.CsvParser
import com.esri.core.geometry._
import com.esri.gdb._
import com.esri.udt.PolylineMType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.{Logging, SparkContext}

object SnapApp extends App with Logging {

  val appFile = args.length match {
    case 0 => "app.properties"
    case _ => args.head
  }
  val conf = AppProperties.loadProperties(appFile)
    .setAppName("SnapApp")
    .set("spark.app.id", "SnapApp")
    .registerKryoClasses(Array(
      classOf[Feature],
      classOf[FeatureFold],
      classOf[FeatureMulti],
      classOf[FeaturePoint],
      classOf[RowCol],
      classOf[Snap],
      classOf[SnapInfo],
      classOf[SnapLine],
      classOf[SnapM],
      classOf[SnapXY],
      classOf[Point],
      classOf[Polyline]
    ))

  val sc = new SparkContext(conf)
  try {
    val cellSize = conf.getDouble("cell.size", 0.01)
    val snapDist = conf.getDouble("snap.dist", 0.001)

    val multiPath = conf.get("gdb.path", "/Volumes/SSD512G/TXData/2014/TXDOT_Roadway_Inventory.gdb")
    val multiName = conf.get("gdb.nane", "TXDOT_Roadway_Linework_Routed")

    val pointPath = conf.get("point.path", "/Volumes/SSD512G/TXData/points.csv")
    val outputPath = conf.get("output.path", "/tmp/wkt")
    val outputFormat = conf.get("output.format", "wkt")

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val lhsRDD = sqlContext
      .gdbFile(multiPath, multiName)
      .select("Shape", "OBJECTID")
      .flatMap(row => {
        val p = row.getAs[PolylineMType](0)
        val o = row.getInt(1)
        FeatureMulti(p.asPolyline, Array(o.toString))
          .toRowCols(cellSize, snapDist)
      })

    val schema = new StructType(
      Array(
        StructField("OBJECTID", StringType, nullable = true),
        StructField("LON", DoubleType, nullable = true),
        StructField("LAT", DoubleType, nullable = true)
      )
    )

    val rhsRDD = new CsvParser()
      .withSchema(schema)
      .withUseHeader(false)
      .withParserLib("UNIVOCITY")
      .withParseMode("DROPMALFORMED")
      .csvFile(sqlContext, pointPath)
      .select("OBJECTID", "LON", "LAT")
      .flatMap(row => {
        val o = row.getString(0)
        val x = row.getDouble(1)
        val y = row.getDouble(2)
        FeaturePoint(new Point(x, y), Array(o))
          .toRowCols(cellSize, snapDist)
      })

    val outRDD = Snapper.snap(lhsRDD, rhsRDD, snapDist)

    outputFormat match {
      case "avro" =>
        outRDD.toDF().write.avro(outputPath)
      case "parquet" =>
        outRDD.toDF().write.parquet(outputPath)
      case _ =>
        outRDD.map(si => {
          val attr = si.attr.mkString("\t")
          val side = si.side match {
            case 0 => 'O'
            case 1 => 'R'
            case _ => 'L'
          }
          s"LINESTRING(${si.px} ${si.py},${si.x} ${si.y})\t${si.rho}\t$side\t$attr"
        })
          .saveAsTextFile(outputPath)
    }

  } finally {
    sc.stop()
  }
}
