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

package org.apache.spark.util.kvstore

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.util
import org.bson.Document
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.ServerAddress
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.ReplaceOptions
import org.mongodb.scala.FindObservable

import org.apache.spark.annotation.Private

@Private
class MongodbStore(connStr: String) extends KVStore {

  val settings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings(b => b.hosts(List(new ServerAddress("localhost")).asJava))
    .build()
  val mongoClient: MongoClient = MongoClient(settings)

  val database: MongoDatabase = mongoClient.getDatabase("sparkhistory")

  var metadata: Any = null

  val METADATA_COLLECTION = "metadata"

  val mapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .build()

  val replaceOptions = new ReplaceOptions().upsert(true)

  val jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()

  /**
   * Returns app-specific metadata from the store, or null if it's not currently set.
   *
   * <p>
   * The metadata type is application-specific. This is a convenience method so that applications
   * don't need to define their own keys for this information.
   * </p>
   */
  override def getMetadata[T](klass: Class[T]): T = {
    klass.cast(metadata)
  }

  /**
   * Writes the given value in the store metadata key.
   */
  override def setMetadata(value: Any): Unit = {
    metadata = value
  }

  /**
   * Read a specific instance of an object.
   *
   * @param naturalKey The object's "natural key", which uniquely identifies it. Null keys
   *                   are not allowed.
   * @throws java.util.NoSuchElementException If an element with the given key does not exist.
   */
  override def read[T](klass: Class[T], naturalKey: Any): T = {
    val KVTypeInfo = new KVTypeInfo(klass)
    val query = equal(KVTypeInfo.getFieldOrMethodName(KVIndex.NATURAL_INDEX_NAME), naturalKey)
    val r = database.getCollection(klass.getSimpleName).find(query).first()
    val d = Await.result(r.toFuture(), Duration.Inf)

    if (d == null) {
      throw new NoSuchElementException
    }
    mapper.readValue(d.toJson(jsonWriterSettings).getBytes(), klass)
  }

  /**
   * Writes the given object to the store, including indexed fields. Indices are updated based
   * on the annotated fields of the object's class.
   *
   * <p>
   * Writes may be slower when the object already exists in the store, since it will involve
   * updating existing indices.
   * </p>
   *
   * @param value The object to write.
   */
  override def write(value: Any): Unit = {
    val json = mapper.writeValueAsString(value)
    val KVTypeInfo = new KVTypeInfo(value.getClass)
    val query = equal(KVTypeInfo.getFieldOrMethodName(KVIndex.NATURAL_INDEX_NAME),
      KVTypeInfo.getAccessor(KVIndex.NATURAL_INDEX_NAME).get(value))
    val result =
      database
      .getCollection[Document](value.getClass.getSimpleName)
      .replaceOne(query, Document.parse(json), replaceOptions)
    Await.result(result.toFuture(), Duration.Inf)
  }

  /**
   * Removes an object and all data related to it, like index entries, from the store.
   *
   * @param type       The object's type.
   * @param naturalKey The object's "natural key", which uniquely identifies it. Null keys
   *                   are not allowed.
   * @throws java.util.NoSuchElementException If an element with the given key does not exist.
   */
  override def delete(`type`: Class[_], naturalKey: Any): Unit = {
    val KVTypeInfo = new KVTypeInfo(`type`)
    val result =
      database
        .getCollection[Document](`type`.getSimpleName)
        .deleteOne(equal(KVTypeInfo.getFieldOrMethodName(KVIndex.NATURAL_INDEX_NAME), naturalKey))
    Await.result(result.toFuture(), Duration.Inf)
  }

  /**
   * Returns a configurable view for iterating over entities of the given type.
   */
  override def view[T](`type`: Class[T]): KVStoreView[T] = {
    val r = database.getCollection[Document](`type`.getSimpleName).find()
    new MongoView[T](r, `type`)
  }

  /**
   * Returns the number of items of the given type currently in the store.
   */
  override def count(`type`: Class[_]): Long = {
    val result = database.getCollection(`type`.getSimpleName).countDocuments()
    Await.result(result.toFuture(), Duration.Inf)
  }

  /**
   * Returns the number of items of the given type which match the given indexed value.
   */
  override def count(`type`: Class[_], index: String, indexedValue: Any): Long = {
    val KVTypeInfo = new KVTypeInfo(`type`)
    val result = database
      .getCollection(`type`.getSimpleName)
      .countDocuments(equal(KVTypeInfo.getFieldOrMethodName(index), indexedValue))
    Await.result(result.toFuture(), Duration.Inf)
  }

  /**
   * A cheaper way to remove multiple items from the KVStore
   */
  override def removeAllByIndexValues[T](klass: Class[T], index: String, indexValues: util.Collection[_]): Boolean = {
    val KVTypeInfo = new KVTypeInfo(klass)

    val result =
      database
        .getCollection[Document](klass.getSimpleName)
        .deleteMany(in(KVTypeInfo.getFieldOrMethodName(index), indexValues.toArray))

    Await.result(result.toFuture(), Duration.Inf).getDeletedCount >= 0
  }

  override def close(): Unit = {}

  class MongoView[T](observable: FindObservable[Document], klass: Class[T]) extends KVStoreView[T] {
    val KVTypeInfo = new KVTypeInfo(klass)

    override def iterator(): util.Iterator[T] = {
      var iterator = observable
      if (first != null) {
        iterator = iterator.filter(gte(KVTypeInfo.getFieldOrMethodName(index), first))
      }

      if (last != null) {
        iterator = iterator.filter(lt(KVTypeInfo.getFieldOrMethodName(index), first))
      }

      val limit = if (max == Long.MaxValue) 0 else max.toInt
      iterator = iterator.limit(limit).skip(skip.toInt)

      Await.result(iterator.toFuture(), Duration.Inf)
        .map(e => mapper.readValue(e.toJson(jsonWriterSettings).getBytes, klass))
        .toIterator
        .asJava

    }
  }
}
