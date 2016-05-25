package com.esri

trait Snap extends Serializable {
  def x: Double

  def y: Double

  def distanceOnLine: Double

  def distanceToLine: Double

  def rho: Double // 1-d/D

  def side: Byte // TODO - use const 99:NoSnap, 0:OnLine, 1:Right, -1:Left

  def >(that: Snap) = this.rho > that.rho

  def snapped() = 99 != side
}
