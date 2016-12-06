package com.esri

/**
  * @deprecated
  */
trait Feature extends Serializable {
  def toRowCols(cellSize: Double, snapMaxDistance: Double): Seq[(RowCol, Feature)]
}
