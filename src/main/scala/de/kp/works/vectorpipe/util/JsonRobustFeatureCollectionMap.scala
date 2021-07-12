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

import scala.collection.mutable
import scala.reflect.ClassTag

class JsonRobustFeatureCollectionMap(features: List[Json] = Nil) {
  private val buffer = mutable.ListBuffer(features:_*)

  def add[G <: Geometry: ClassTag, D: Encoder](featureMap: (String, RobustFeature[G, D])): buffer.type =
    buffer += RobustFeatureFormats.writeRobustFeatureJsonWithID(featureMap)

  def asJson: Json = {
    val bboxOption = getAll[Geometry].map(_._2.extent).reduceOption(_ combine _)
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

  private def getFeatureID(js: Json): String = {
    val c = js.hcursor
    val id = c.downField("id")
    id.as[String] match {
      case Right(i) => i
      case _ =>
        id.as[Int] match {
          case Right(i) => i.toString
          case _ => throw DecodingFailure("Feature expected to have \"ID\" field", c.history)
        }
    }
  }

  def getAll[F: Decoder]: Map[String, F] = {
    var ret = Map[String, F]()
    features.foreach{ f => f.as[F].foreach(ret += getFeatureID(f) -> _) }
    ret
  }
}

object JsonRobustFeatureCollectionMap {
  def apply() = new JsonRobustFeatureCollectionMap()

  def apply[G <: Geometry: ClassTag, D: Encoder](features: Traversable[(String, RobustFeature[G, D])]): JsonRobustFeatureCollectionMap = {
    val fc = new JsonRobustFeatureCollectionMap()
    features.foreach(fc.add(_))
    fc
  }

  def apply(features: Traversable[Json])(implicit d: DummyImplicit): JsonRobustFeatureCollectionMap =
    new JsonRobustFeatureCollectionMap(features.toList)
}
