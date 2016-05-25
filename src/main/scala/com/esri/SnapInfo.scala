package com.esri

case class SnapInfo(px: Double,
                    py: Double,
                    x: Double,
                    y: Double,
                    distanceOnLine: Double,
                    distanceToLine: Double,
                    rho: Double,
                    side: Byte,
                    attr: Array[String]) extends Snap