package de.kp.works.vectorpipe.vectortile
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

import geotrellis.layer.SpatialKey
import geotrellis.spark.store.hadoop._
import geotrellis.spark.store.s3._
import geotrellis.vectortile._
import org.apache.spark.rdd.RDD

import software.amazon.awssdk.services.s3.model.ObjectCannedACL

import java.net.URI
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

package object export {
  def saveVectorTiles(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, uri: URI): Unit = {
    uri.getScheme match {
      case "s3" =>
        val path = uri.getPath
        val prefix = path.stripPrefix("/").stripSuffix("/")
        saveToS3(vectorTiles, zoom, uri.getAuthority, prefix)
      case _ =>
        saveHadoop(vectorTiles, zoom, uri)
    }
  }

  private def saveToS3(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String): Unit = {
    vectorTiles
      .mapValues { tile =>
        val byteStream = new ByteArrayOutputStream()

        try {
          val gzipStream = new GZIPOutputStream(byteStream)
          try {
            gzipStream.write(tile.toBytes)
          } finally {
            gzipStream.close()
          }
        } finally {
          byteStream.close()
        }

        byteStream.toByteArray
      }
      .saveToS3(
        { sk: SpatialKey => s"s3://$bucket/$prefix/$zoom/${sk.col}/${sk.row}.mvt" },
        putObjectModifier = { request =>
          request
            .toBuilder
            .contentEncoding("gzip")
            .acl(ObjectCannedACL.PUBLIC_READ)
            .build()
        })
  }

  private def saveHadoop(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, uri: URI) = {
    vectorTiles
      .mapValues(_.toBytes)
      .saveToHadoop({ sk: SpatialKey => s"${uri}/$zoom/${sk.col}/${sk.row}.mvt" })
  }

}
