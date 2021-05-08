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

import org.apache.spark.status.api.v1.FailureSummary
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.jobs.ApiHelper.errorMessageCell

import scala.xml.Node

private[spark] object InsightUIUtils {

  def failureSummaryTable(failureSummary: Seq[FailureSummary]): Seq[Node] = {
    val propertyHeader = Seq("Exception", "Message", "Count", "Details")
    val headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
    UIUtils.listingTable(propertyHeader, failureSummaryRow,
      failureSummary,
      headerClasses = headerClasses, fixedWidth = true)
  }

  def failureSummaryRow(e: FailureSummary): Seq[Node] = {
    <tr>
      <td>
        {e.exceptionFailure.failureType}
      </td>
      <td>
        {e.exceptionFailure.message}
      </td>
      <td>
        {e.count}
      </td>{errorMessageCell(e.exceptionFailure.stackTrace)}
    </tr>
  }
}