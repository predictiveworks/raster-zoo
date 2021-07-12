package de.kp.works.vectorpipe.model
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

import de.kp.works.vectorpipe.internal.{NodeType, RelationType, WayType}

import scala.xml.Node

case class Member(`type`: Byte, ref: Long, role: String)

object Member {
  def typeFromString(str: String): Byte = str match {
      case "node"     => NodeType
      case "way"      => WayType
      case "relation" => RelationType
      case _ => null.asInstanceOf[Byte]
  }

  def stringFromByte(b: Byte): String = b match {
      case NodeType     => "node"
      case WayType      => "way"
      case RelationType => "relation"
  }

  def fromXML(node: Node): Member = {
    val `type` = typeFromString(node \@ "type")
    val ref = (node \@ "ref").toLong
    val role = node \@ "role"

    Member(`type`, ref, role)
  }
}
