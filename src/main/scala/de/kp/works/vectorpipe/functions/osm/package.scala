package de.kp.works.vectorpipe.functions
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

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

import de.kp.works.vectorpipe.model.Member
import de.kp.works.vectorpipe.util._

import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

package object osm {
  /**
   * Using tag listings from
   * [id-area-keys](https://github.com/osmlab/id-area-keys) @ v2.13.0.
   */
  val AreaKeys: Map[String, Map[String, Boolean]] = Map(
    "addr:*" -> Map(),
    "advertising" -> Map(
      "billboard" -> true
    ),
    "aerialway" -> Map(
      "cable_car" -> true,
      "chair_lift" -> true,
      "drag_lift" -> true,
      "gondola" -> true,
      "goods" -> true,
      "magic_carpet" -> true,
      "mixed_lift" -> true,
      "platter" -> true,
      "rope_tow" -> true,
      "t-bar" -> true
    ),
    "aeroway" -> Map(
      "runway" -> true,
      "taxiway" -> true
    ),
    "allotments" -> Map(),
    "amenity" -> Map(
      "bench" -> true
    ),
    "area:highway" -> Map(),
    "attraction" -> Map(
      "dark_ride" -> true,
      "river_rafting" -> true,
      "summer_toboggan" -> true,
      "train" -> true,
      "water_slide" -> true
    ),
    "bridge:support" -> Map(),
    "building" -> Map(),
    "camp_site" -> Map(),
    "club" -> Map(),
    "craft" -> Map(),
    "emergency" -> Map(
      "designated" -> true,
      "destination" -> true,
      "no" -> true,
      "official" -> true,
      "private" -> true,
      "yes" -> true
    ),
    "golf" -> Map(
      "hole" -> true,
      "lateral_water_hazard" -> true,
      "water_hazard" -> true
    ),
    "healthcare" -> Map(),
    "historic" -> Map(),
    "industrial" -> Map(),
    "internet_access" -> Map(),
    "junction" -> Map(
      "circular" -> true,
      "roundabout" -> true
    ),
    "landuse" -> Map(),
    "leisure" -> Map(
      "slipway" -> true,
      "track" -> true
    ),
    "man_made" -> Map(
      "breakwater" -> true,
      "crane" -> true,
      "cutline" -> true,
      "embankment" -> true,
      "groyne" -> true,
      "pier" -> true,
      "pipeline" -> true
    ),
    "military" -> Map(),
    "natural" -> Map(
      "cliff" -> true,
      "coastline" -> true,
      "ridge" -> true,
      "tree_row" -> true
    ),
    "office" -> Map(),
    "piste:type" -> Map(
      "downhill" -> true,
      "hike" -> true,
      "ice_skate" -> true,
      "nordic" -> true,
      "skitour" -> true,
      "sled" -> true,
      "sleigh" -> true
    ),
    "place" -> Map(),
    "playground" -> Map(
      "balancebeam" -> true,
      "slide" -> true,
      "zipwire" -> true
    ),
    "power" -> Map(
      "cable" -> true,
      "line" -> true,
      "minor_line" -> true
    ),
    "public_transport" -> Map(
      "platform" -> true
    ),
    "residential" -> Map(),
    "seamark:type" -> Map(),
    "shop" -> Map(),
    "tourism" -> Map(
      "artwork" -> true
    ),
    "traffic_calming" -> Map(
      "bump" -> true,
      "cushion" -> true,
      "dip" -> true,
      "hump" -> true,
      "rumble_strip" -> true
    ),
    "waterway" -> Map(
      "canal" -> true,
      "dam" -> true,
      "ditch" -> true,
      "drain" -> true,
      "river" -> true,
      "stream" -> true,
      "weir" -> true
    )
  )

  private val MultiPolygonTypes = Seq("multipolygon", "boundary")

  /* __MOD__ Public access enabled */
  val TruthyValues = Seq("yes", "true", "1")

  /* __MOD__ Public access enabled */
  val FalsyValues = Seq("no", "false", "0")

  /* __MOD__ Public access enabled */
  val BooleanValues = TruthyValues ++ FalsyValues

  private val WaterwayValues =
    Seq(
      "river", "riverbank", "canal", "stream", "stream_end", "brook", "drain", "ditch", "dam", "weir", "waterfall",
      "pressurised"
    )

  private val POITags = Set("amenity", "shop", "craft", "office", "leisure", "aeroway")

  private val HashtagMatcher: Regex = """#([^\u2000-\u206F\u2E00-\u2E7F\s\\'!\"#$%()*,.\/;<=>?@\[\]^{|}~]+)""".r

  private def cleanDelimitedValues(values: Column): Column = regexp_replace(trim(values), "\\s*;\\s*", ";")

  def splitDelimitedValues(values: Column, default: Column = lit("")): Column = split(lower(coalesce(cleanDelimitedValues(values), default)), ";")

  def splitDelimitedValues(values: String): Set[String] =
    values.replaceAll("\\s*;\\s*", ";").toLowerCase().split(";").toSet

  private val _isArea = (tags: Map[String, String]) =>
    tags match {
      case _ if tags.contains("area") && BooleanValues.toSet.intersect(splitDelimitedValues(tags("area"))).nonEmpty =>

        TruthyValues.toSet.intersect(splitDelimitedValues(tags("area"))).nonEmpty
      case _ =>
        // see https://github.com/osmlab/id-area-keys (values are inverted)
        val matchingKeys = tags.keySet.intersect(AreaKeys.keySet)

        matchingKeys.exists(k => {
          // break out semicolon-delimited values
          val values = splitDelimitedValues(tags(k))

          // values that should be considered as lines
          AreaKeys(k).keySet
            .intersect(values)
            // at least one key passes the area test
            .size < values.size
        })
    }

  val isAreaUDF: UserDefinedFunction = udf(_isArea)

  def isArea(tags: Column): Column = isAreaUDF(tags) as 'isArea

  def isMultiPolygon(tags: Column): Column =
    array_intersects(
      splitDelimitedValues(tags.getItem("type")),
      lit(MultiPolygonTypes.toArray)) as 'isMultiPolygon

  def isNew(version: Column, minorVersion: Column): Column =
    version <=> 1 && minorVersion <=> 0 as 'isNew

  def isRoute(tags: Column): Column =
    array_contains(splitDelimitedValues(tags.getItem("type")), "route") as 'isRoute

  private lazy val MemberSchema = ArrayType(
    StructType(
      StructField("type", ByteType, nullable = false) ::
        StructField("ref", LongType, nullable = false) ::
        StructField("role", StringType, nullable = false) ::
        Nil), containsNull = false)

  private val _compressMemberTypes = (members: Seq[Row]) =>
    members.map { row =>
      val t = Member.typeFromString(row.getAs[String]("type"))
      val ref = row.getAs[Long]("ref")
      val role = row.getAs[String]("role")

      Row(t, ref, role)
    }

  @transient lazy val compressMemberTypes: UserDefinedFunction = udf(_compressMemberTypes, MemberSchema)

  /**
   * Checks if members have byte-encoded types
   */
  def hasCompressedMemberTypes(input: DataFrame): Boolean = {
    Try(input.schema("members")
             .dataType
             .asInstanceOf[ArrayType]
             .elementType
             .asInstanceOf[StructType]
             .apply("type")) match {
      case Failure(_) => false
      case Success(field) => field.dataType == ByteType
    }
  }

  def ensureCompressedMembers(input: DataFrame): DataFrame = {
    if (hasCompressedMemberTypes(input))
      input
    else {
      input.withColumn("members", compressMemberTypes(col("members")))
    }
  }

  case class StrMember(`type`: String, ref: Long, role: String)

  private val elaborateMembers = org.apache.spark.sql.functions.udf { member: Seq[Row] =>
    if (member == null)
      null
    else {
      member.map { row: Row =>
        StrMember(de.kp.works.vectorpipe.model.Member.stringFromByte(row.getAs[Byte]("type")),
                  row.getAs[Long]("ref"),
                  row.getAs[String]("role"))
      }
    }
  }

  // matches letters or emoji (no numbers or punctuation)
  private val ContentMatcher: Regex = """[\p{L}\uD83C-\uDBFF\uDC00-\uDFFF]""".r
  private val TrailingPunctuationMatcher: Regex = """[:]$""".r

  @transient lazy val extractHashtags: UserDefinedFunction = udf { comment: String =>
    HashtagMatcher
      .findAllMatchIn(comment)
      // fetch the first group (after #)
      .map(_.group(1).toLowerCase)
      // check that each group contains at least one substantive character
      .filter(ContentMatcher.findFirstIn(_).isDefined)
      // strip trailing punctuation
      .map(TrailingPunctuationMatcher.replaceAllIn(_, ""))
      .toList // prevent a Stream from being returned
      .distinct
  }

  def hashtags(comment: Column): Column =
    // only call the UDF when necessary
    when(comment.isNotNull and length(comment) > 0, extractHashtags(comment))
      .otherwise(typedLit(Seq.empty[String])) as 'hashtags

  def isTagged(tags: Column): Column = size(map_keys(tags)) > 0 as 'isTagged

  def isBuilding(tags: Column): Column =
    !lower(coalesce(tags.getItem("building"), lit("no"))).isin(FalsyValues: _*) as 'isBuilding

  @transient lazy val isPOI: UserDefinedFunction = udf {
    tags: Map[String, String] => POITags.intersect(tags.keySet).nonEmpty
  }

  def isRoad(tags: Column): Column =
    tags.getItem("highway").isNotNull as 'isRoad

  def isCoastline(tags: Column): Column =
    array_contains(splitDelimitedValues(tags.getItem("natural")), "coastline") as 'isCoastline

  def isWaterway(tags: Column): Column =
    array_intersects(splitDelimitedValues(tags.getItem("waterway")), lit(WaterwayValues.toArray)) as 'isWaterway

  def mergeTags: UserDefinedFunction = udf { (a: Map[String, String], b: Map[String, String]) =>
    mergeMaps(a.mapValues(Set(_)), b.mapValues(Set(_)))(_ ++ _).mapValues(_.mkString(";"))
  }

  val reduceTags: UserDefinedFunction = udf { tags: Iterable[Map[String, String]] =>
    tags.map(x => x.mapValues(Set(_))).reduce((a, b) => mergeMaps(a, b)(_ ++ _)).mapValues(_.mkString(";"))
  }

  val array_intersects: UserDefinedFunction = udf { (a: Seq[_], b: Seq[_]) =>
    a.intersect(b).nonEmpty}

  // from the top 200 single-use tags in 20190610's history dump
  // select k, count(*) from history
  // cross join unnest(map_keys(tags)) as t (k)
  // where cardinality(tags) = 1
  // group by k
  // order by count(*) desc
  val UninterestingTags: Set[String] = Set(
    "created_by",
    "source",
    "comment",
    "_ID",
    "CLC",
    "odbl",
    "origen",
    "converted_by",
    "todo",
    "import_tools",
    "ID",
    "importuuid",
    "attribution",
    "curve_geometry",
    "memphis_fixup",
    "importance",
    "description=ru-mos-325",
    "stamv??g",
    "_FID_",
    "1",
    "_description_",
    "ccpr",
    "dfg",
    // https://github.com/gravitystorm/openstreetmap-carto/blob/master/openstreetmap-carto.lua#L51
    "source_ref",
    "ref:UrbIS",
    "accuracy:meters",
    "sub_sea:type",
    "waterway:type",
    "statscan:rbuid",
    "ref:ruian:addr",
    "ref:ruian",
    "building:ruian:type",
    "dibavod:id",
    "uir_adr:ADRESA_KOD",
    "gst:feat_id",
    "maaamet:ETAK",
    "ref:FR:FANTOIR",
    "3dshapes:ggmodelk",
    "AND_nosr_r",
    "OPPDATERIN",
    "addr:city:simc",
    "addr:street:sym_ul",
    "building:usage:pl",
    "building:use:pl",
    "teryt:simc",
    "raba:id",
    "dcgis:gis_id",
    "nycdoitt:bin",
    "chicago:building_id",
    "lojic:bgnum",
    "massgis:way_id",
    "OBJTYPE",
    "SK53_bulk:load"
  ).map(_.toLowerCase())

  val SemiInterestingTags: Set[String] = Set("source").map(_.toLowerCase())

  val UninterestingPrefixes: Set[String] = Set(
    "CLC",
    "tiger",
    "sby",
    "navibot",
    "nps",
    "hoot",
    "error",
    "project",
    // https://github.com/gravitystorm/openstreetmap-carto/blob/master/openstreetmap-carto.lua#L51
    "geobase",
    "canvec",
    "osak",
    "kms",
    "ngbe",
    "it:fvg",
    "KSJ2",
    "yh",
    "LINZ2OSM",
    "LINZ",
    "WroclawGIS",
    "naptap",
    "gnis",
    "NHD",
    "mvdgis"
  ).map(_.toLowerCase())

  val SemiInterestingPrefixes: Set[String] = Set("source").map(_.toLowerCase())

  val UninterestingSingleTags: Set[String] = Set("colour").map(_.toLowerCase())

  lazy val removeUninterestingTags: UserDefinedFunction = udf(_removeUninterestingTags)
  /*
   * __MOD__
   *
   * The `tags` datatype was changed from Map[String,String]
   * to mutable.WrappedArray[Row]
   */
  private val _removeUninterestingTags = (tags: mutable.WrappedArray[Row]) => {

    val mtags = tags.map(tag => {

      val k = new String(tag.getAs[Array[Byte]]("key"))
      val v = new String(tag.getAs[Array[Byte]]("value"))

      (k,v)

    }).toMap

    mtags.filterKeys(key => {
      val k = key.toLowerCase
      !UninterestingTags.contains(k) &&
        !(tags.size == 1 && UninterestingSingleTags.contains(k)) &&
        !UninterestingPrefixes.exists(p => k.startsWith(s"$p:")) &&
        !k.contains("=") &&
        !k.contains(" ")
    })

  }

  lazy val removeSemiInterestingTags: UserDefinedFunction = udf(_removeSemiInterestingTags)

  private val _removeSemiInterestingTags = (tags: Map[String, String]) =>
    tags.filterKeys(key => {
      val k = key.toLowerCase
      !SemiInterestingTags.contains(k) &&
        !SemiInterestingPrefixes.exists(p => k.startsWith(s"$p:"))
    })
}
