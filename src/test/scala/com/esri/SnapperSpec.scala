package com.esri

import com.esri.core.geometry.{Point, Polyline}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  */
class SnapperSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val config = new SparkConf()
      .setMaster("local")
      .setAppName("MainSpec")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")
    sc = new SparkContext(config)
  }

  it should "no snap" in {
    val cellSize = 5.0
    val maxDist = 2.0
    val polyline = new Polyline()
    polyline.startPath(-10, 0)
    polyline.lineTo(10, 0)
    val lhs = sc.parallelize(Seq(FeatureMulti(polyline, Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val rhs = sc.parallelize(Seq(FeaturePoint(new Point(0, 10), Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val res = Snapper.snap(lhs, rhs, maxDist).collect()

    res shouldBe empty
  }

  it should "snap to 0,0" in {
    val cellSize = 5.0
    val maxDist = 2.0
    val polyline = new Polyline()
    polyline.startPath(-10, 0)
    polyline.lineTo(10, 0)
    val lhs = sc.parallelize(Seq(FeatureMulti(polyline, Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val rhs = sc.parallelize(Seq(FeaturePoint(new Point(0, 1), Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val res = Snapper.snap(lhs, rhs, maxDist).collect()

    res should not be empty
    val snapInfo = res.head
    snapInfo.px shouldBe 0.0
    snapInfo.py shouldBe 1.0
    snapInfo.x shouldBe 0.0
    snapInfo.y shouldBe 0.0
    snapInfo.distanceToLine shouldBe 1.0
    snapInfo.distanceOnLine shouldBe 2.0 // 10.0
  }

  it should "snap to 4,4.5" in {
    val cellSize = 5.0
    val maxDist = 2.0
    val polyline = new Polyline()
    polyline.startPath(-10, 4.5)
    polyline.lineTo(10, 4.5)
    val lhs = sc.parallelize(Seq(FeatureMulti(polyline, Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val rhs = sc.parallelize(Seq(FeaturePoint(new Point(0, 5.5), Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val res = Snapper.snap(lhs, rhs, maxDist).collect()

    res should not be empty
    res should have length (1)
    val snapInfo = res.head
    snapInfo.px shouldBe 0.0
    snapInfo.py shouldBe 5.5
    snapInfo.x shouldBe 0.0
    snapInfo.y shouldBe 4.5
    snapInfo.distanceToLine shouldBe 1.0
    snapInfo.distanceOnLine shouldBe 2.0 // 10.0
  }

  it should "snap to 5.5,5" in {
    val cellSize = 5.0
    val maxDist = 2.0
    val polyline = new Polyline()
    polyline.startPath(5.5, 0)
    polyline.lineTo(5.5, 10.0)
    val lhs = sc.parallelize(Seq(FeatureMulti(polyline, Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val rhs = sc.parallelize(Seq(FeaturePoint(new Point(4.5, 5.0), Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val res = Snapper.snap(lhs, rhs, maxDist).collect()

    res should not be empty
    res should have length (1)
    val snapInfo = res.head
    snapInfo.px shouldBe 4.5
    snapInfo.py shouldBe 5.0
    snapInfo.x shouldBe 5.5
    snapInfo.y shouldBe 5.0
    snapInfo.distanceToLine shouldBe 1.0
    snapInfo.distanceOnLine shouldBe 2.0 // 5.0
  }

  it should "snap to 5,5" in {
    val cellSize = 5.0
    val maxDist = 2.0
    val polyline = new Polyline()
    polyline.startPath(5, 0)
    polyline.lineTo(5, 10)
    val lhs = sc.parallelize(Seq(FeatureMulti(polyline, Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val rhs = sc.parallelize(Seq(FeaturePoint(new Point(5.0, 5.0), Array.empty[String])).flatMap(_.toRowCols(cellSize, maxDist)))
    val res = Snapper.snap(lhs, rhs, maxDist).collect()

    res should not be empty
    res should have length (1)
    val snapInfo = res.head
    snapInfo.px shouldBe 5.0
    snapInfo.py shouldBe 5.0
    snapInfo.x shouldBe 5.0
    snapInfo.y shouldBe 5.0
    snapInfo.distanceToLine shouldBe 0.0
    snapInfo.distanceOnLine shouldBe 2.0 // 5.0
  }

  it should "snap to 5.5,5.0 (2)" in {
    val cellSize = 5.0
    val maxDist = 2.0

    val polyline1 = new Polyline()
    polyline1.startPath(5.5, 0)
    polyline1.lineTo(5.5, 10)

    val polyline2 = new Polyline()
    polyline2.startPath(0.0, 4.0)
    polyline2.lineTo(10.0, 4.0)

    val lhs = sc.parallelize(Seq(
      FeatureMulti(polyline1, Array("poly1")),
      FeatureMulti(polyline2, Array("poly2"))
    ).flatMap(_.toRowCols(cellSize, maxDist)))

    val rhs = sc.parallelize(Seq(FeaturePoint(new Point(5.0, 5.0), Array.empty[String]))
      .flatMap(_.toRowCols(cellSize, maxDist)))

    val res = Snapper.snap(lhs, rhs, maxDist).collect()

    res should not be empty
    res should have length (1)
    val snapInfo = res.head
    snapInfo.px shouldBe 5.0
    snapInfo.py shouldBe 5.0
    snapInfo.x shouldBe 5.5
    snapInfo.y shouldBe 5.0
    snapInfo.distanceToLine shouldBe 0.5
    snapInfo.distanceOnLine shouldBe 2.0 // 5.0

    snapInfo.attr shouldBe Array("poly1")
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
    } finally
      super.afterAll()
  }
}
