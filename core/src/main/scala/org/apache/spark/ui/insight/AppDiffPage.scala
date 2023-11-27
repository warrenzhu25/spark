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

package org.apache.spark.ui.insight

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.ApplicationHistoryProvider
import org.apache.spark.ui._

private[ui] class AppDiffPage(
    parent: AppDiffTab,
    conf: SparkConf,
    historyProvider: ApplicationHistoryProvider) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val appIds: Seq[String] = Option(request.getParameterValues("appIds"))
      .getOrElse(Array[String]()).toSeq
    val statusStoreByAppIds = appIds.map(a => a -> historyProvider.getAppStatusStore(a)).toMap
    val content = <div></div>
    UIUtils.headerSparkPage(request, "AppDiff", content, parent)
  }
}
private[ui] class AppDiffTab(
    parent: SparkUI,
    historyProvider: ApplicationHistoryProvider) extends SparkUITab(parent, "Diff") {
  attachPage(new AppDiffPage(this, parent.conf, historyProvider))
}
