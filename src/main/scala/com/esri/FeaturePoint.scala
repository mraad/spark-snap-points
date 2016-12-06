package com.esri

import com.esri.core.geometry.Point

case class FeaturePoint(val point: Point, val attr: Array[String]) extends Serializable {

  def x() = point.getX

  def y() = point.getY

  def m() = point.getM

  def toRowCols(cellSize: Double, snapMaxDistance: Double) = {
    val c = (point.getX / cellSize).floor.toInt
    val r = (point.getY / cellSize).floor.toInt
    Seq((RowCol(r, c), this))
  }
}
