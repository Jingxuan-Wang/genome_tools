/**
  * Copyright © DataSpark Pte Ltd 2014 - 2015.
  * This software and any related documentation contain confidential and proprietary information of DataSpark and its licensors (if any).
  * Use of this software and any related documentation is governed by the terms of your written agreement with DataSpark.
  * You may not use, download or install this software or any related documentation without obtaining an appropriate licence agreement from DataSpark.
  * All rights reserved.
  */
package geojson

import com.esri.core.geometry._

// $COVERAGE-OFF$
/**
  * A wrapper that provides convenience methods for using the spatial relations in the ESRI
  * GeometryEngine with a particular instance of the Geometry interface and an associated
  * SpatialReference.
  *
  * @param geometry the geometry object
  * @param csr      optional spatial reference; if not specified, uses WKID 4326 a.k.a. WGS84, the
  *                 standard coordinate frame for Earth.
  */
class RichGeometry(val geometry: Geometry,
                   val csr: SpatialReference = SpatialReference.create(4326)) extends Serializable {

  def area2D(): Double = geometry.calculateArea2D()

  def distance(other: Geometry): Double = {
    GeometryEngine.distance(geometry, other, csr)
  }

  def contains(other: Geometry): Boolean = {
    GeometryEngine.contains(geometry, other, csr)
  }

  def within(other: Geometry): Boolean = {
    GeometryEngine.within(geometry, other, csr)
  }

  def overlaps(other: Geometry): Boolean = {
    GeometryEngine.overlaps(geometry, other, csr)
  }

  def touches(other: Geometry): Boolean = {
    GeometryEngine.touches(geometry, other, csr)
  }

  def crosses(other: Geometry): Boolean = {
    GeometryEngine.crosses(geometry, other, csr)
  }

  def disjoint(other: Geometry): Boolean = {
    GeometryEngine.disjoint(geometry, other, csr)
  }

  def geodesicDistanceOnWGS84(other: Geometry): Double = {
    (geometry, other) match {
      case (p1: Point, p2: Point) => GeometryEngine.geodesicDistanceOnWGS84(geometry.asInstanceOf[Point], other.asInstanceOf[Point])
      case _ => throw new Exception("Require points")
    }

  }

  def getType(): Geometry.Type = geometry.getType()
}

/**
  * Helper object for implicitly creating RichGeometry wrappers
  * for a given Geometry instance.
  */
object RichGeometry extends Serializable {
  implicit def createRichGeometry(g: Geometry): RichGeometry = new RichGeometry(g)
}

// $COVERAGE-ON$