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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, NodeSeq}

import org.apache.spark.internal.config.UI._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "executors") {

  val sc = parent.sc

  init()

  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)
    val heapHistogramEnabled =
      parent.sc.isDefined && parent.conf.get(UI_HEAP_HISTOGRAM_ENABLED)

    attachPage(new ExecutorsPage(this, threadDumpEnabled, heapHistogramEnabled, store))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
    if (heapHistogramEnabled) {
      attachPage(new ExecutorHeapHistogramPage(this, parent.sc))
    }
  }

}

private[ui] class ExecutorsPage(
    parent: ExecutorsTab,
    threadDumpEnabled: Boolean,
    heapHistogramEnabled: Boolean,
    store: AppStatusStore)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val appInfo = store.applicationInfo()

    val summary: NodeSeq =
      <div>
        <ul class="list-unstyled">
          <li>
            <strong>Total Executor Uptime:</strong>{
              appInfo.totalExecutorTime.map(t => UIUtils.formatDuration(t)).getOrElse("N/A")
            }
          </li>

        </ul>
      </div>

    val content =
      {
        <div id="active-executors"></div> ++ summary ++
        <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
        <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
        <script>setThreadDumpEnabled({threadDumpEnabled})</script>
        <script>setHeapHistogramEnabled({heapHistogramEnabled})</script>
      }

    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
}
