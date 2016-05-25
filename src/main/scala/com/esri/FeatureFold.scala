package com.esri

import scala.collection.mutable

case class FeatureFold() {

  private val m_points = mutable.Buffer[FeaturePoint]()
  private val m_lines = mutable.Buffer[FeatureMulti]()

  def points(): Seq[FeaturePoint] = m_points

  def lines(): Seq[FeatureMulti] = m_lines

  def +=(feature: Feature) = {
    feature match {
      case p: FeaturePoint => m_points += p
      case l: FeatureMulti => m_lines += l
      case _ => throw new Exception("Should never ever get here !")
    }
    this
  }

}