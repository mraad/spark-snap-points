package com.esri

case class SnapXY(val x: Double,
                  val y: Double,
                  val distanceOnLine: Double,
                  val distanceToLine: Double,
                  val rho: Double,
                  val side: Byte
                 ) extends Snap
