package com.esri

trait Feature extends Serializable {
  def toRowCols(cellSize: Double, snapMaxDistance: Double): Seq[(RowCol, Feature)]
}
