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
  def header: Seq[String]
  def row: Seq[Node]
}

abstract class HeuristicResult(val name: String, results: Seq[AnalysisResult]) {

  def toTable: Seq[Node] = {
    assert(results.nonEmpty)
    UIUtils.listingTable(results.head.header,
      (r: AnalysisResult) => r.row,
      results,
      fixedWidth = true)
  }
}

case class SingleValue(name: String,
                       value: String,
                       description: String = "",
                       suggested: String = "",
                       override val severity: Severity = Severity.Normal
                       ) extends AnalysisResult {

  override val header: Seq[String] =
    Seq("Name", "Value", "Suggested", "Description", "Severity")

  override val row: Seq[Node] = {
    <tr>
      <td>{name}</td>
      <td>{value}</td>
      <td>{description}</td>
      <td>{suggested}</td>
      <td>
        <span data-toggle="tooltip" title={severity.getTooltip}>
          {severity}
        </span>
      </td>
    </tr>
  }
}

case class MultipleValues(name: String,
                          values: Seq[String],
                          description: Option[String] = None,
                          override val severity: Severity = Severity.Normal
                         ) extends AnalysisResult {
  override val header: Seq[String] =
    Seq("Name", "Value", "Description", "Severity")

  override val row: Elem = {
    <tr>
      <td>{name}</td>
      <td>{values.mkString("\n")}</td>
      <td>{description}</td>
      <td>
        <span data-toggle="tooltip" title={severity.getTooltip}>
          {severity}
        </span>
      </td>
    </tr>
  }
}
