/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.status.insight.heuristics

import scala.xml.{Elem, Node}

import org.apache.spark.ui.UIUtils

trait AnalysisResult {
  def severity: Severity
  def toTuple: (String, String, String, String, String)
}

abstract class HeuristicResult(val name: String, results: Seq[AnalysisResult]) {

  def toTable: Seq[Node] =
    UIUtils.listingTable(insightHeader, insightRow, results.map(_.toTuple)
      , fixedWidth = true)

  protected val insightHeader: Seq[String] =
    Seq("Name", "Value", "Suggested", "Description", "Severity")

  protected def insightRow(data: (String, String, String, String, String)): Elem =
    <tr>
      <td>{data._1}</td>
      <td>{data._2}</td>
      <td>{data._4}</td>
      <td>{data._3}</td>
      <td>
        <span data-toggle="tooltip" title={Severity.valueOf(data._5).getTooltip}>
          {data._5}
        </span>
      </td>
    </tr>
}

case class SingleValue(name: String,
                       value: String,
                       description: String = "",
                       suggested: String = "",
                       override val severity: Severity = Severity.Normal
                       ) extends AnalysisResult {
  override def toTuple(): (String, String, String, String, String) = {
    (name, value, description, suggested, severity.name())
  }
}

case class MultipleValues(name: String,
                          values: Seq[String],
                          description: Option[String] = None,
                          override val severity: Severity = Severity.Normal
                         ) extends AnalysisResult {
  override def toTuple(): (String, String, String, String, String) = {
    (name, values.mkString("\n"), description.getOrElse(""), "", severity.name())
  }
}
