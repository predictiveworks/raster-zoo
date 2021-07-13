package de.kp.works.vectorpipe.util
/**
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Copyright 2011-2017 Azavea [http://www.azavea.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import cats.syntax.either._
import geotrellis.vector._

import _root_.io.circe._
import _root_.io.circe.syntax._

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable

import scala.reflect.ClassTag

class JsonRobustFeatureCollection(features: List[Json] = Nil) {
  private val buffer = mutable.ListBuffer(features: _*)

  def add[G <: Geometry: ClassTag, D: Encoder](feature: RobustFeature[G, D]) =
    buffer += RobustFeatureFormats.writeRobustFeatureJson(feature)

  def getAll[F: Decoder]: Vector[F] = {
    val ret = new VectorBuilder[F]()
    features.foreach{ _.as[F].foreach(ret += _) }
    ret.result()
  }

  def getAllRobustFeatures[F <: RobustFeature[_, _] :Decoder]: Vector[F] =
    getAll[F]

  def getAllPointFeatures[D: Decoder](): Seq[RobustFeature[Point, D]] = 
    getAll[RobustFeature[Point, D]]
  
  def getAllLineStringFeatures[D: Decoder](): Seq[RobustFeature[LineString, D]] = 
    getAll[RobustFeature[LineString, D]]

  def getAllPolygonFeatures[D: Decoder](): Seq[RobustFeature[Polygon, D]] = 
    getAll[RobustFeature[Polygon, D]]

  def getAllMultiPointFeatures[D: Decoder](): Seq[RobustFeature[MultiPoint, D]] =
    getAll[RobustFeature[MultiPoint, D]]

  def getAllMultiLineStringFeatures[D: Decoder](): Seq[RobustFeature[MultiLineString, D]] =
    getAll[RobustFeature[MultiLineString, D]]

  def getAllMultiPolygonFeatures[D: Decoder](): Seq[RobustFeature[MultiPolygon, D]] =
    getAll[RobustFeature[MultiPolygon, D]]

  def getAllGeometries(): Vector[Geometry] =
    getAll[Point] ++ getAll[LineString] ++ getAll[Polygon] ++
      getAll[MultiPoint] ++ getAll[MultiLineString] ++ getAll[MultiPolygon]

  def asJson: Json = {
    val bboxOption = getAllGeometries().map(_.extent).reduceOption(_ combine _)
    bboxOption match {
      case Some(bbox) =>
        Json.obj(
          "type" -> "FeatureCollection".asJson,
          "bbox" -> Extent.listEncoder(bbox),
          "features" -> buffer.toVector.asJson
        )
      case _ =>
        Json.obj(
          "type" -> "FeatureCollection".asJson,
          "features" -> buffer.toVector.asJson
        )
    }
  }
}

object JsonRobustFeatureCollection {
  def apply() = new JsonRobustFeatureCollection()

  def apply[G <: Geometry: ClassTag, D: Encoder](features: Traversable[RobustFeature[G, D]]): JsonRobustFeatureCollection = {
    val fc = new JsonRobustFeatureCollection()
    features.foreach(fc.add(_))
    fc
  }

  def apply(features: Traversable[Json])(implicit d: DummyImplicit): JsonRobustFeatureCollection =
    new JsonRobustFeatureCollection(features.toList)
}
