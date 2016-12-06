package com.esri

import com.esri.core.geometry.{Envelope2D, Geometry, MultiPath, OperatorClip}
import spire.implicits.cfor

import scala.collection.mutable.ArrayBuffer

case class FeatureMulti(val geom: Geometry, val attr: Array[String]) extends Serializable {

  def asMultiPath() = geom.asInstanceOf[MultiPath]

  def toRowCols(cellSize: Double, snapMaxDistance: Double) = {
    val arr = new ArrayBuffer[(RowCol, FeatureMulti)]()
    val envp = new Envelope2D()
    geom.queryEnvelope2D(envp)
    val cmin = ((envp.xmin - snapMaxDistance) / cellSize).floor.toInt
    val cmax = ((envp.xmax + snapMaxDistance) / cellSize).floor.toInt
    val rmin = ((envp.ymin - snapMaxDistance) / cellSize).floor.toInt
    val rmax = ((envp.ymax + snapMaxDistance) / cellSize).floor.toInt
    /*
        for (r <- rmin to rmax; c <- cmin to cmax) {
          yield (RowCol(r, c), this)
        }
    */
    val clip = OperatorClip.local
    cfor(rmin)(_ <= rmax, _ + 1)(r => {
      cfor(cmin)(_ <= cmax, _ + 1)(c => {
        val x = c * cellSize
        val y = r * cellSize
        envp.xmin = x - snapMaxDistance
        envp.xmax = x + cellSize + snapMaxDistance
        envp.ymin = y - snapMaxDistance
        envp.ymax = y + cellSize + snapMaxDistance
        val clipped = clip.execute(geom, envp, null, null)
        if (!clipped.isEmpty) {
          arr += RowCol(r, c) -> FeatureMulti(clipped, attr)
        }
      })
    })
    arr
  }
}
