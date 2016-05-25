package com.esri

case class SnapM(val x: Double,
                 val y: Double,
                 val m: Double,
                 val distanceOnLine: Double,
                 val distanceToLine: Double,
                 val rho: Double,
                 val side: Byte
                ) extends Snap

