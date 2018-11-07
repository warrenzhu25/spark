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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.microsoft.nao.infra.MdmReporter

import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.SecurityManager

class MdmSink(val property: Properties, val registry: MetricRegistry,
              securityMgr: SecurityManager) extends Sink {
  val MDM_DEFAULT_PERIOD = 1
  val MDM_DEFAULT_UNIT = "MINUTES"

  val MDM_KEY_MONITORING_ACCOUNT = "monitoringAccount"
  val MDM_KEY_METRIC_NAMESPACE = "metricNamespace"
  val MDM_KEY_PERIOD = "period"
  val MDM_KEY_UNIT = "unit"

  val monitoringAccount = Option(property.getProperty(MDM_KEY_MONITORING_ACCOUNT)) match {
    case Some(s) => s
    case None => ""
  }

  val metricNamespace = Option(property.getProperty(MDM_KEY_METRIC_NAMESPACE)) match {
    case Some(s) => s
    case None => ""
  }

  val pollPeriod = Option(property.getProperty(MDM_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => MDM_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(MDM_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(MDM_DEFAULT_UNIT)
  }

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: MdmReporter = MdmReporter.forRegistry(registry)
    .overrideMonitoringAccount(this.monitoringAccount)
    .overrideMetricNamespace(this.metricNamespace)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
