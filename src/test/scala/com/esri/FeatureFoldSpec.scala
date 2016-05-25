package com.esri

import com.esri.core.geometry.{Point, Polyline}
import org.scalatest.{FlatSpec, Matchers}

/**
  */
class FeatureFoldSpec extends FlatSpec with Matchers {
  it should "create a point and line set" in {
    val ff = Seq(
      FeaturePoint(new Point(0, 0), Array.empty[String]),
      FeatureMulti(new Polyline(), Array.empty[String])
    )
      .foldLeft(FeatureFold())(_ += _)
    ff.points.isEmpty shouldBe false
    ff.lines.isEmpty shouldBe false
  }
}
