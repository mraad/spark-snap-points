package com.esri

import com.esri.core.geometry.{Envelope2D, MultiPath}

case class FeatureMulti(val multiPath: MultiPath, val attr: Array[String]) extends Feature {

  override def toRowCols(cellSize: Double, snapMaxDistance: Double): Seq[(RowCol, Feature)] = {
    val envp = new Envelope2D()
    multiPath.queryEnvelope2D(envp)
    val cmin = ((envp.xmin - snapMaxDistance) / cellSize).floor.toInt
    val cmax = ((envp.xmax + snapMaxDistance) / cellSize).floor.toInt
    val rmin = ((envp.ymin - snapMaxDistance) / cellSize).floor.toInt
    val rmax = ((envp.ymax + snapMaxDistance) / cellSize).floor.toInt
    for (r <- rmin to rmax; c <- cmin to cmax)
      yield (RowCol(r, c), this)
  }

}
