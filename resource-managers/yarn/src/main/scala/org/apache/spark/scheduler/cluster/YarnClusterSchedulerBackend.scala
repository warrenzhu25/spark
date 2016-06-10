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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils
import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.{ApplicationMaster, YarnSparkHadoopUtil}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc) {

  override def start() {
    val attemptId = ApplicationMaster.getAttemptId
    bindToYarn(attemptId.getApplicationId(), Some(attemptId))
    super.start()
    totalExpectedExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(sc.conf)
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var driverLogs: Option[Map[String, String]] = None
    try {
      val yarnConf = new YarnConfiguration(sc.hadoopConfiguration)
      val containerId = YarnSparkHadoopUtil.getContainerId

      var httpAddress = System.getenv(Environment.NM_HOST.name()) +
        ":" + System.getenv(Environment.NM_HTTP_PORT.name())
      // lookup appropriate http scheme for container log urls
      val yarnHttpPolicy = yarnConf.get(
        YarnConfiguration.YARN_HTTP_POLICY_KEY,
        YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
      )
      val user = Utils.getCurrentUserName()
      val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"
      val proxyServer = sc.getConf.getOption("spark.event.proxy")
      // enable log accessible outside AP through proxy
      if (proxyServer.isDefined) {
        val machineAddress = proxyServer.get
        val baseUrl = s"$httpScheme$machineAddress/node/containerlogs/$containerId/$user"
        logDebug(s"Base URL for logs: $baseUrl")
        val ipRegex = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])(:\\d{1,5})?$"
        if (!httpAddress.matches(ipRegex)) {
          // since some mathined Id such as BN2SCH030340532:8042 can not be resolved through proxy,
          // we need to enforce it ending with .phx.gbl:8042
          val machineReg = sc.getConf.getOption("spark.machine.reg")
          if (machineReg.isDefined) {
            val machineRegex = machineReg.get.toString.r
            if (machineRegex.findAllIn(httpAddress).isEmpty) {
              httpAddress = httpAddress.split(':')(0) + machineRegex
            }
          }
        }
        driverLogs = Some(Map(
          "stderr" -> s"$baseUrl/stderr?start=-4096&machineId=$httpAddress",
          "stdout" -> s"$baseUrl/stdout?start=-4096&machineId=$httpAddress"))
      } else if (yarnConf.getBoolean(YarnConfiguration.NM_WEBAPP_PROXY_ENABLED, false)) {
        val proxyAddress = yarnConf.get(YarnConfiguration.PROXY_ADDRESS,
          YarnConfiguration.DEFAULT_PROXY_ADDRESS)
        val proxyBase = ProxyUriUtils.NM_PROXY_BASE
        val addressBase = System.getenv(Environment.NM_HOST.name())
        val addressPort = System.getenv(Environment.NM_HTTP_PORT.name())
        val baseUrl = s"$httpScheme$proxyAddress$proxyBase$addressBase" +
          s"/$addressPort/node/containerlogs/$containerId/$user"
        logDebug(s"Base URL for logs: $baseUrl")
        driverLogs = Some(Map(
          "stderr" -> s"$baseUrl/stderr?start=-4096",
          "stdout" -> s"$baseUrl/stdout?start=-4096"))
      } else {
        val baseUrl = s"$httpScheme$httpAddress/node/containerlogs/$containerId/$user"
        logDebug(s"Base URL for logs: $baseUrl")
        driverLogs = Some(Map(
          "stdout" -> s"$baseUrl/stdout?start=-4096",
          "stderr" -> s"$baseUrl/stderr?start=-4096"))
      }
    } catch {
      case e: Exception =>
        logInfo("Error while building AM log links, so AM" +
          " logs link will not appear in application UI", e)
    }
    driverLogs
  }
}
