package com.esri

import com.esri.core.geometry.{Point, Polyline}
import org.scalatest._

/**
  */
class SnapperXYSpec extends FlatSpec with Matchers {

  def rot(c: Double, s: Double, tx: Double, ty: Double, x: Double, y: Double) = {
    ((c * x - s * y) + tx, (s * x + c * y) + ty)
  }

  val p1 = new Point(0, 0)
  val p2 = new Point(100, 0)
  val poly = new Polyline()
  poly.startPath(p1)
  poly.lineTo(p2)

  it should "not snap -20,0" in {
    val snap = Snapper.snapXY(poly, -20, 0, 10)

    snap.snapped shouldBe false
    snap.rho shouldBe 0.0
    snap.side shouldBe 99
  }

  it should "not snap 120,0" in {
    val snap = Snapper.snapXY(poly, 120, 0, 10)

    snap.snapped shouldBe false
    snap.rho shouldBe 0.0
    snap.side shouldBe 99
  }

  it should "not snap 50,20" in {
    val snap = Snapper.snapXY(poly, 50, 20, 10)

    snap.snapped shouldBe false
    snap.rho shouldBe 0.0
    snap.side shouldBe 99
  }

  it should "not snap 50,-20" in {
    val snap = Snapper.snapXY(poly, 50, -20, 10)

    snap.snapped shouldBe false
    snap.rho shouldBe 0.0
    snap.side shouldBe 99
  }

  it should "snap -5,-5" in {
    val snap = Snapper.snapXY(poly, -5, -5, 10)

    snap.rho shouldBe 1.0 - Math.sqrt(25 + 25) / 10
    snap.side shouldBe 1
    snap.distanceOnLine shouldBe 0.0
    snap.distanceToLine shouldBe Math.sqrt(25 + 25)
    snap.x shouldBe 0
    snap.y shouldBe 0
  }

  it should "snap -5,0" in {
    val snap = Snapper.snapXY(poly, -5, 0, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe 0
    snap.distanceOnLine shouldBe 0.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 0
    snap.y shouldBe 0
  }

  it should "snap -5,5" in {
    val snap = Snapper.snapXY(poly, -5, 5, 10)

    snap.rho shouldBe 1.0 - Math.sqrt(25 + 25) / 10
    snap.side shouldBe -1
    snap.distanceOnLine shouldBe 0.0
    snap.distanceToLine shouldBe Math.sqrt(25 + 25)
    snap.x shouldBe 0
    snap.y shouldBe 0
  }

  it should "snap 0,-5" in {
    val snap = Snapper.snapXY(poly, 0, -5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe 1
    snap.distanceOnLine shouldBe 0.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 0
    snap.y shouldBe 0
  }

  it should "snap 0,0" in {
    val snap = Snapper.snapXY(poly, 0, 0, 10)

    snap.rho shouldBe 1.0
    snap.side shouldBe 0
    snap.distanceOnLine shouldBe 0.0
    snap.distanceToLine shouldBe 0.0
    snap.x shouldBe 0
    snap.y shouldBe 0
  }

  it should "snap 0,5" in {
    val snap = Snapper.snapXY(poly, 0, 5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe -1
    snap.distanceOnLine shouldBe 0.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 0
    snap.y shouldBe 0
  }

  it should "snap 5,-5" in {
    val snap = Snapper.snapXY(poly, 5, -5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe 1
    snap.distanceOnLine shouldBe 5.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 5
    snap.y shouldBe 0
  }

  it should "snap 5,0" in {
    val snap = Snapper.snapXY(poly, 5, 0, 10)

    snap.rho shouldBe 1.0
    snap.side shouldBe 0
    snap.distanceOnLine shouldBe 5.0
    snap.distanceToLine shouldBe 0.0
    snap.x shouldBe 5
    snap.y shouldBe 0
  }

  it should "snap 5,5" in {
    val snap = Snapper.snapXY(poly, 5, 5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe -1
    snap.distanceOnLine shouldBe 5.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 5
    snap.y shouldBe 0
  }

  it should "snap 95,-5" in {
    val snap = Snapper.snapXY(poly, 95, -5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe 1
    snap.distanceOnLine shouldBe 95.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 95
    snap.y shouldBe 0
  }

  it should "snap 95,0" in {
    val snap = Snapper.snapXY(poly, 95, 0, 10)

    snap.rho shouldBe 1.0
    snap.side shouldBe 0
    snap.distanceOnLine shouldBe 95.0
    snap.distanceToLine shouldBe 0.0
    snap.x shouldBe 95
    snap.y shouldBe 0
  }

  it should "snap 95,5" in {
    val snap = Snapper.snapXY(poly, 95, 5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe -1
    snap.distanceOnLine shouldBe 95.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 95
    snap.y shouldBe 0
  }

  it should "snap 100,-5" in {
    val snap = Snapper.snapXY(poly, 100, -5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe 1
    snap.distanceOnLine shouldBe 100.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 100
    snap.y shouldBe 0
  }

  it should "snap 100,0" in {
    val snap = Snapper.snapXY(poly, 100, 0, 10)

    snap.rho shouldBe 1.0
    snap.side shouldBe 0
    snap.distanceOnLine shouldBe 100.0
    snap.distanceToLine shouldBe 0.0
    snap.x shouldBe 100
    snap.y shouldBe 0
  }

  it should "snap 100,5" in {
    val snap = Snapper.snapXY(poly, 100, 5, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe -1
    snap.distanceOnLine shouldBe 100.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 100
    snap.y shouldBe 0
  }

  it should "snap 105,-5" in {
    val snap = Snapper.snapXY(poly, 105, -5, 10)

    snap.rho shouldBe 1.0 - Math.sqrt(50.0) / 10.0
    snap.side shouldBe 1
    snap.distanceOnLine shouldBe 100.0
    snap.distanceToLine shouldBe Math.sqrt(50.0)
    snap.x shouldBe 100
    snap.y shouldBe 0
  }

  it should "snap 105,0" in {
    val snap = Snapper.snapXY(poly, 105, 0, 10)

    snap.rho shouldBe 0.5
    snap.side shouldBe 0
    snap.distanceOnLine shouldBe 100.0
    snap.distanceToLine shouldBe 5.0
    snap.x shouldBe 100
    snap.y shouldBe 0
  }

  it should "snap 105,5" in {
    val snap = Snapper.snapXY(poly, 105, 5, 10)

    snap.rho shouldBe 1.0 - Math.sqrt(50.0) / 10.0
    snap.side shouldBe -1
    snap.distanceOnLine shouldBe 100.0
    snap.distanceToLine shouldBe Math.sqrt(50.0)
    snap.x shouldBe 100
    snap.y shouldBe 0
  }

  it should "snap to 10,1" in {

    val eps = 1e-6

    val r = new util.Random(System.currentTimeMillis)

    for (t <- 0 to 100) {
      val a = (-180.0 * 360.0 * r.nextDouble).toRadians
      val c = Math.cos(a)
      val s = Math.sin(a)
      val tx = 100 * r.nextDouble
      val ty = 100 * r.nextDouble

      val ((ax, ay), am) = (rot(c, s, tx, ty, 0.0, 0.0), 0.0)
      val ((bx, by), bm) = (rot(c, s, tx, ty, 10.0, 0.0), 10.0)
      val ((cx, cy), cm) = (rot(c, s, tx, ty, 10.0, 10.0), 20.0)
      val (px, py) = rot(c, s, tx, ty, 9.5, 1.0)
      val ((qx, qy), qm) = (rot(c, s, tx, ty, 10.0, 1.0), 11)

      val p1 = new Point(ax, ay)
      p1.setM(am)
      val p2 = new Point(bx, by)
      p2.setM(bm)
      val p3 = new Point(cx, cy)
      p3.setM(cm)

      val poly = new Polyline()
      poly.startPath(p1)
      poly.lineTo(p2)
      poly.lineTo(p3)

      val snap = Snapper.snapXY(poly, px, py, 2.0)

      snap.rho shouldBe ((1.0 - 0.5 / 2.0) +- eps)
      snap.side shouldBe -1
      snap.distanceOnLine shouldBe (11.0 +- eps)
      snap.distanceToLine shouldBe (0.5 +- eps)
      snap.x shouldBe (qx +- eps)
      snap.y shouldBe (qy +- eps)
    }
  }
}
