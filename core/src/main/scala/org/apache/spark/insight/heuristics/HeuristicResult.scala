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
package org.apache.spark.insight.heuristics

import scala.xml.Node

import org.apache.spark.ui.UIUtils

case class AnalysisRecord(
    name: String,
    value: String,
    description: String = "",
    suggested: String = "",
    severity: Severity = Severity.Normal) {

  val header: Seq[String] =
    Seq("Name", "Value", "Suggested", "Description", "Severity")

  def toHTML(basePathUri: String): Seq[Node] = {
    <tr>
      <td>{name}</td>
      <td>{value}</td>
      <td>{suggested}</td>
      <td>{description}</td>
      <td>
        <span data-toggle="tooltip" title={severity.getTooltip}>
          {severity}
        </span>
      </td>
    </tr>
  }
}

case class AnalysisResult(
    records: Seq[AnalysisRecord],
    title: String => Seq[Node] = (_ => Seq.empty),
    severity: Severity = Severity.Normal) {
  val header: Seq[String] =
    records.head.header

  def toHTML(basePathUri: String): Seq[Node] = {
    assert(records.nonEmpty)
    title.apply(basePathUri) ++
    UIUtils.listingTable(
      header,
      (r: AnalysisRecord) => r.toHTML(basePathUri),
      records,
      fixedWidth = true)
  }

}

case class HeuristicResult(name: String, results: Seq[AnalysisResult]) {

  def toHTML(basePathUri: String): Seq[Node] = {
    <span class="collapse-aggregated-classpathEntries collapse-table"
          onClick="collapseTable('collapse-aggregated-classpathEntries',
            'aggregated-classpathEntries')">
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>{name}</a>
      </h4>
    </span>
        <div class="aggregated-classpathEntries collapsible-table">
          {results.map(r => r.toHTML(basePathUri))}
        </div>
  }
}
