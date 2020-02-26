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

package org.apache.spark.sql.pii

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import org.apache.spark.internal.config.{ConfigEntry, ConfigReader, OptionalConfigEntry}
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

object PIIConf extends Logging {

  private def adminConfigReader = new ConfigReader(
    adminProperties.getOrElse(adminConfProperties).asJava)

  // Visible for testing
  private [sql] var adminProperties: Option[Map[String, String]] = None

  private lazy val adminConfProperties = {
    val properties = Utils.getAdminProperties()
    logInfo(s"Admin config are $properties")
    properties
  }

  // Admin config set by platform. Can not override by user
  private val SUPPORT_PII = SQLConf.buildStaticConf("spark.admin.pii.support")
    .doc("Whether this cluster can hold pii data")
    .booleanConf
    .createWithDefault(false)

  private val PII_DATA_PATH = SQLConf.buildStaticConf("spark.admin.pii.path")
    .doc("PII data path")
    .stringConf
    .createOptional

  private val PII_METADATA_DB_HOST = SQLConf.buildStaticConf("spark.admin.pii.metadata.db.host")
    .doc("PII metadata database host")
    .stringConf
    .createOptional

  private val PII_METADATA_DB_NAME = SQLConf.buildStaticConf("spark.admin.pii.metadata.db.name")
    .doc("PII metadata database name")
    .stringConf
    .createOptional

  private val PII_METADATA_DB_USER = SQLConf.buildStaticConf("spark.admin.pii.metadata.db.user")
    .doc("PII metadata database user")
    .stringConf
    .createOptional

  private val PII_OPTIONAL_CONFS: Seq[OptionalConfigEntry[String]] =
    Seq(PII_DATA_PATH, PII_METADATA_DB_HOST, PII_METADATA_DB_NAME, PII_METADATA_DB_USER)

  // User config
  private val USE_PII = SQLConf.buildConf("spark.pii.enabled")
    .doc("When true, enable the ability to access PII data")
    .booleanConf
    .createWithDefault(false)

  def isUsePII(conf: RuntimeConfig) = {
    isPIISupported && conf.get(USE_PII)
  }

  def isPIISupported = {
    val supportPII = get(SUPPORT_PII)

    if (supportPII) {
      PII_OPTIONAL_CONFS.foreach(requireConfDefined)
    }

    supportPII
  }

  private lazy val password: String = {
    new String(SparkHadoopUtil.get.conf.getPassword("pii_metadata"))
  }

  def piiDataPath = {
    require(get(SUPPORT_PII), "piiDataPath can only be called when spark.admin.pii.support=true,")
    get(PII_DATA_PATH).get
  }

  def metadataConnString = {
    {
      require(get(SUPPORT_PII), "metadataConnString can only be called when spark.admin.pii.support=true,")

      val hostName = get(PII_METADATA_DB_HOST).get
      val dbName = get(PII_METADATA_DB_NAME).get
      val user = get(PII_METADATA_DB_USER).get

      s"jdbc:sqlserver://$hostName:1433;database=$dbName;user=$user;" +
        s"password=$password;encrypt=true;" +
        s"hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    }
  }

  def requireConfDefined(entry: OptionalConfigEntry[String]): Unit = {
    require(get(entry).isDefined, s"spark.admin.pii.supported=true but ${entry.key} is empty")
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[ConfigEntry]].
   */
  def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(adminConfigReader)
  }

  /**
   * Return the value of an optional Spark SQL configuration property for the given key. If the key
   * is not set yet, returns None.
   */
  def get[T](entry: OptionalConfigEntry[T]): Option[T] = {
    entry.readFrom(adminConfigReader)
  }
}
