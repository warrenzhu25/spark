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

package org.apache.spark.ui.insights

import scala.xml.Node

import org.apache.spark.insight.heuristics.DiagnosisResult
import org.apache.spark.insight.heuristics.FailureDiagnosis
import org.apache.spark.status.api.v1.FailureSummary
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.jobs.ApiHelper.errorMessageCell

private[spark] object InsightUIUtils {

  def failureSummaryTable(failureSummary: Seq[FailureSummary]): Seq[Node] = {
    val propertyHeader = Seq("Exception", "Message", "Count", "Details", "Diagnosis",
      "IsRootCause", "Suggestion")
    val headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
    UIUtils.listingTable(propertyHeader, failureSummaryRow,
      failureSummary,
      headerClasses = headerClasses, fixedWidth = true)
  }

  def failureSummaryRow(e: FailureSummary): Seq[Node] = {
    <tr>
      <td>{e.exceptionFailure.failureType}</td>
      <td>{e.exceptionFailure.message}</td>
      <td>{e.count}</td>
      {errorMessageCell(e.exceptionFailure.stackTrace)}
      {diagnosisRow(FailureDiagnosis.analysis(e.exceptionFailure))}
    </tr>
  }

  def diagnosisRow(diagnosisResult: Option[DiagnosisResult]): Seq[Node] = {
    diagnosisResult match {
      case Some(d) =>
        <td>{d.desc}</td>
          <td>{d.rootCause}</td>
          <td>{configsCell(d)}</td>
      case _ =>
        <td></td>
          <td></td>
          <td></td>
    }
  }

  def configsCell(diagnosisResult: DiagnosisResult): Seq[Node] = {
    if (diagnosisResult.suggestedConfigs.nonEmpty) {
      diagnosisResult.suggestedConfigs
        .map(_.productIterator.mkString("="))
        .map(s => <p>{s}</p>)
        .toSeq ++ copyToClipboardButton(diagnosisResult)
    } else {
      Seq.empty
    }
  }

  def copyToClipboardButton(diagnosisResult: DiagnosisResult): Seq[Node] = {
    <button
    type="button"
    class="btn btn-default btn-copy js-tooltip js-copy"
    data-toggle="tooltip"
    data-placement="bottom"
    data-copy={toSparkSubmitConf(diagnosisResult.suggestedConfigs)}
    diagnosis-type={diagnosisResult.diagnosisType}
    title="Copy to clipboard">Copy spark-submit conf
    </button>
  }

  def toSparkSubmitConf(configs: Map[String, String]): String = {
    configs
      .map(_.productIterator.mkString("="))
      .map(s => s"--conf $s")
      .mkString(" ")
  }
}
