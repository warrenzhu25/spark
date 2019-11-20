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

import com.codahale.metrics.{Counter, Gauge, Metric, MetricRegistry}
import com.microsoft.nao.infra.{Mdm, MdmReporter}
import org.mockito.Matchers.{any, anyLong, anyString}
import org.mockito.Mockito._
import org.mockito.internal.util.reflection.FieldSetter
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkFunSuite}

class MdmSinkSuite extends SparkFunSuite with  BeforeAndAfter{

  private case class MdmMetric(name: String, value: Long, dimensionsValue: Array[String]);

  private val inputAppName = "testApp"
  private val inputAppId = "applicationId"
  private val inputExeId = "executorId"

  private var sink : MdmSink = null
  private var mockMdm : Mdm = null
  private var mdmMetric : MdmMetric = null

  before {

    // Set App name in SparkConf
    val sparkConf = new SparkConf(false)
    sparkConf.set("spark.app.name", inputAppName)

    // Mock SparkEnv
    val mockSparkEnv = mock(classOf[SparkEnv])
    SparkEnv.set(mockSparkEnv)
    doReturn(sparkConf).when(mockSparkEnv).conf

    val properties = new Properties
    properties.setProperty("metricPattern",
      s"^(?!$inputAppId\\.$inputExeId\\.excludedMetricPart1\\.).*$$")

    sink = new MdmSink(properties, new MetricRegistry, new SecurityManager(sparkConf))

    // Mock Mdm
    mockMdm = mock(classOf[Mdm])

    doAnswer(
      new Answer[Unit] () {
        override def answer(i: InvocationOnMock): Unit = {
          mdmMetric = new MdmMetric(
            i.getArgumentAt(0, classOf[String]),
            i.getArgumentAt(3, classOf[Long]),
            i.getArgumentAt(2, classOf[Array[String]]))
        }
      }).when(mockMdm)
      .ReportMetric3D(anyString, any(classOf[Array[String]]), any(classOf[Array[String]]), anyLong)

    new FieldSetter(sink.reporter, classOf[MdmReporter].getDeclaredField("mdm")).set(mockMdm)
  }

  after{
    mdmMetric = null
  }


  test("metric Mdm Sink with Gauge") {
    val gauge = new Gauge[Double] {
      override def getValue: Double = 1.23
    }

    val inputMetricName = "metricPart1.metricPart2"
    val fullMetricName = MetricRegistry.name(inputAppId, inputExeId, inputMetricName)
    sink.registry.register(fullMetricName, gauge)
    sink.report()

    assert(mdmMetric.name.equals(inputMetricName))
    assert(mdmMetric.dimensionsValue)
    assert(mdmMetric.value == gauge.getValue.toLong)
  }

  test("metric Mdm Sink with Counter") {
    val counter = new Counter
    counter.inc(12)

    val inputMetricName = "metricPart1.metricPart2"
    val fullMetricName = MetricRegistry.name(inputAppId, inputExeId, inputMetricName)
    sink.registry.register(fullMetricName, counter)
    sink.report()

    assert(mdmMetric.name.equals(inputMetricName))
    assert(mdmMetric.dimensionsValue)
    assert(mdmMetric.value == counter.getCount)
  }

  test("metric Mdm Sink about usage") {
    val gauge = new Gauge[Double] {
      override def getValue: Double = 0.235
    }

    val inputMetricName = "metricPart1.usage"
    val fullMetricName = MetricRegistry.name(inputAppId, inputExeId, inputMetricName)
    sink.registry.register(fullMetricName, gauge)
    sink.report()

    assert(mdmMetric.name.equals(inputMetricName))
    assert(mdmMetric.dimensionsValue)

    // Metrics end with ".usage" represents a percentage.
    // Mdm only accepts Long, do a trick by multiplying 100
    assert(mdmMetric.value == (gauge.getValue * 100 ).toLong)
  }

  test("metric Mdm Sink with filter") {
    val inputMetricName = "excludedMetricPart1.excludedMetricPart2"
    val fullMetricName = MetricRegistry.name(inputAppId, inputExeId, inputMetricName)
    sink.registry.register(fullMetricName, new Metric() {})
    sink.report()

    assert(mdmMetric == null)
  }

  private def assert(mdmDimensionsValue: Array[String]): Unit = {
    assert(mdmDimensionsValue(0).equals(inputAppId))
    assert(mdmDimensionsValue(1).equals(inputAppName))
    assert(mdmDimensionsValue(2).equals(inputExeId))
  }
}
