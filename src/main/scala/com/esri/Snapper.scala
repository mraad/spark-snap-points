package com.esri

import com.esri.core.geometry.{MultiPath, Point}
import org.apache.spark.rdd.RDD

object Snapper extends Serializable {

  private def distance(ax: Double, ay: Double, bx: Double, by: Double) = {
    val dx = bx - ax
    val dy = by - ay
    Math.sqrt(dx * dx + dy * dy)
  }

  /**
    * http://stackoverflow.com/questions/849211/shortest-distance-between-a-point-and-a-line-segment
    * http://mathworld.wolfram.com/Point-LineDistance2-Dimensional.html
    */
  def snapXY(multiPath: MultiPath, px: Double, py: Double, maxDist: Double) = {
    var accLen = 0.0
    var minLen = 0.0
    var minRho = 0.0
    var minSide = 99.asInstanceOf[Byte]
    var minDist = Double.PositiveInfinity
    var qx = px
    var qy = py
    val si = multiPath.querySegmentIterator()
    si.resetToFirstPath()
    while (si.nextPath) {
      while (si.hasNextSegment) {
        val segment = si.nextSegment
        val dx = segment.getEndX - segment.getStartX
        val dy = segment.getEndY - segment.getStartY
        val lenSqr = dx * dx + dy * dy
        if (lenSqr > 0.0) {
          val segLen = Math.sqrt(lenSqr)
          val fraction = ((px - segment.getStartX) * dx + (py - segment.getStartY) * dy) / lenSqr
          val cross = (segment.getStartY - py) * dx - (segment.getStartX - px) * dy
          val segDist = if (fraction <= 0.0) distance(segment.getStartX, segment.getStartY, px, py)
          else if (fraction >= 1.0) distance(segment.getEndX, segment.getEndY, px, py)
          else Math.abs(cross) / segLen
          if (segDist < maxDist && segDist < minDist) {
            minDist = segDist
            minSide = if (cross < 0.0) -1 else if (cross > 0.0) 1 else 0
            minRho = 1.0 - segDist / maxDist
            if (fraction <= 0.0) {
              minLen = accLen
              qx = segment.getStartX
              qy = segment.getStartY
            } else if (fraction >= 1.0) {
              minLen = accLen + segLen
              qx = segment.getEndX
              qy = segment.getEndY
            } else {
              minLen = accLen + segLen * fraction
              qx = segment.getStartX + fraction * dx
              qy = segment.getStartY + fraction * dy
            }
          }
          accLen += segLen
        }
      }
    }
    SnapXY(qx, qy, minLen, minDist, minRho, minSide)
  }


  def snapM(multiPath: MultiPath, px: Double, py: Double, maxDist: Double) = {
    var accLen = 0.0
    var minLen = 0.0
    var minRho = 0.0
    var minSide = 99.asInstanceOf[Byte]
    var minDist = Double.PositiveInfinity
    var qx = px
    var qy = py
    var qm = 0.0
    val p1 = new Point()
    val p2 = new Point()
    val si = multiPath.querySegmentIterator()
    si.resetToFirstPath()
    while (si.nextPath) {
      while (si.hasNextSegment) {
        val segment = si.nextSegment
        segment.queryStart(p1)
        segment.queryEnd(p2)
        val dx = segment.getEndX - segment.getStartX
        val dy = segment.getEndY - segment.getStartY
        val dm = p2.getM - p1.getM
        val lenSqr = dx * dx + dy * dy
        if (lenSqr > 0.0) {
          val segLen = Math.sqrt(lenSqr)
          val fraction = ((px - segment.getStartX) * dx + (py - segment.getStartY) * dy) / lenSqr
          val cross = (segment.getStartY - py) * dx - (segment.getStartX - px) * dy
          val segDist = if (fraction <= 0.0) distance(segment.getStartX, segment.getStartY, px, py)
          else if (fraction >= 1.0) distance(segment.getEndX, segment.getEndY, px, py)
          else Math.abs(cross) / segLen
          if (segDist < maxDist && segDist < minDist) {
            minDist = segDist
            minSide = if (cross < 0.0) -1 else if (cross > 0.0) 1 else 0
            minRho = 1.0 - segDist / maxDist
            if (fraction <= 0.0) {
              minLen = accLen
              qx = segment.getStartX
              qy = segment.getStartY
              qm = p1.getM
            } else if (fraction >= 1.0) {
              minLen = accLen + segLen
              qx = segment.getEndX
              qy = segment.getEndY
              qm = p2.getM
            } else {
              minLen = accLen + segLen * fraction
              qx = segment.getStartX + fraction * dx
              qy = segment.getStartY + fraction * dy
              qm = p1.getM + fraction * dm
            }
          }
          accLen += segLen
        }
      }
    }
    SnapM(qx, qy, qm, minLen, minDist, minRho, minSide)
  }

  /*
    def snap(lhsRDD: RDD[(RowCol, Feature)],
             rhsRDD: RDD[(RowCol, Feature)],
             maxDist: Double
            ): RDD[SnapInfo] = {
      lhsRDD
        .union(rhsRDD)
        .groupByKey()
        .flatMap {
          case (rowcol, iter) => {
            val fold = iter.foldLeft(FeatureFold())(_ += _)
            fold
              .points
              .flatMap(point => {
                val px = point.x
                val py = point.y
                fold
                  .lines
                  .foldLeft(Option.empty[SnapLine])((prev, line) => {
                    val snap = Snapper.snapXY(line.multiPath, px, py, maxDist)
                    if (snap.snapped) {
                      prev match {
                        case Some(prevSnapLine) => if (prevSnapLine.snap > snap) prev else Some(SnapLine(snap, line))
                        case _ => Some(SnapLine(snap, line))
                      }
                    } else
                      prev
                  })
                  .map(snapLine => {
                    SnapInfo(px, py,
                      snapLine.snap.x,
                      snapLine.snap.y,
                      snapLine.snap.distanceOnLine,
                      snapLine.snap.distanceToLine,
                      snapLine.snap.rho,
                      snapLine.snap.side,
                      point.attr ++ snapLine.featureMulti.attr)
                  })
              })
          }
        }
    }
  */

  def snap(multiRDD: RDD[(RowCol, FeatureMulti)],
           pointRDD: RDD[(RowCol, FeaturePoint)],
           maxDist: Double
          ): RDD[SnapInfo] = {
    multiRDD
      .cogroup(pointRDD)
      .flatMap {
        case (_, (multiIter, pointIter)) => {
          val multiArr = multiIter.toArray
          pointIter
            .flatMap(point => {
              multiArr
                .foldLeft(Option.empty[SnapLine])((prev, multi) => {
                  val snap = Snapper.snapXY(multi.asMultiPath, point.x, point.y, maxDist)
                  if (snap.snapped) {
                    prev match {
                      case Some(prevSnapLine) => if (prevSnapLine.snap > snap) prev else Some(SnapLine(snap, multi))
                      case _ => Some(SnapLine(snap, multi))
                    }
                  } else
                    prev
                })
                .map(snapLine => {
                  SnapInfo(point.x, point.y,
                    snapLine.snap.x,
                    snapLine.snap.y,
                    snapLine.snap.distanceOnLine,
                    snapLine.snap.distanceToLine,
                    snapLine.snap.rho,
                    snapLine.snap.side,
                    point.attr ++ snapLine.featureMulti.attr)
                })
            })
        }
      }
  }
}
