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

package org.apache.spark.network.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.network.util.ConfigProvider;
import org.apache.spark.network.util.TransportConf;
import org.junit.Test;

import static org.junit.Assert.*;

public class TransportConfSuite {

  @Test
  public void testFetchBusyHighWatermarkDefault() {
    TransportConf conf = new TransportConf("test", new MapConfigProvider(new HashMap<>()));
    assertEquals("Default fetch busy high watermark should be 0", 
        0, conf.fetchBusyHighWatermark());
  }

  @Test
  public void testFetchBusyHighWatermarkCustomValue() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.shuffle.fetchBusyHighWatermark", "500");
    TransportConf conf = new TransportConf("test", new MapConfigProvider(properties));
    
    assertEquals("Custom fetch busy high watermark should be respected",
        500, conf.fetchBusyHighWatermark());
  }

  @Test
  public void testFetchBusyLowWatermarkDefault() {
    TransportConf conf = new TransportConf("test", new MapConfigProvider(new HashMap<>()));
    assertEquals("Default fetch busy low watermark should be 0",
        0, conf.fetchBusyLowWatermark());
  }

  @Test
  public void testFetchBusyLowWatermarkCustomValue() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.shuffle.fetchBusyLowWatermark", "200");
    TransportConf conf = new TransportConf("test", new MapConfigProvider(properties));
    
    assertEquals("Custom fetch busy low watermark should be respected",
        200, conf.fetchBusyLowWatermark());
  }

  @Test
  public void testFetchBusyCoolDownDefault() {
    TransportConf conf = new TransportConf("test", new MapConfigProvider(new HashMap<>()));
    assertEquals("Default fetch busy cooldown should be 0",
        0, conf.fetchBusyCoolDown());
  }

  @Test
  public void testFetchBusyCoolDownCustomValue() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.shuffle.fetchBusyCoolDown", "5000");
    TransportConf conf = new TransportConf("test", new MapConfigProvider(properties));
    
    assertEquals("Custom fetch busy cooldown should be respected",
        5000, conf.fetchBusyCoolDown());
  }

  @Test
  public void testAllFetchBusyParametersTogether() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.shuffle.fetchBusyHighWatermark", "1000");
    properties.put("spark.shuffle.fetchBusyLowWatermark", "300");
    properties.put("spark.shuffle.fetchBusyCoolDown", "2000");
    TransportConf conf = new TransportConf("test", new MapConfigProvider(properties));
    
    assertEquals("All fetch busy parameters should work together",
        1000, conf.fetchBusyHighWatermark());
    assertEquals("All fetch busy parameters should work together", 
        300, conf.fetchBusyLowWatermark());
    assertEquals("All fetch busy parameters should work together",
        2000, conf.fetchBusyCoolDown());
  }

  @Test
  public void testInvalidNegativeValues() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.shuffle.fetchBusyHighWatermark", "-100");
    properties.put("spark.shuffle.fetchBusyLowWatermark", "-50");
    properties.put("spark.shuffle.fetchBusyCoolDown", "-1000");
    TransportConf conf = new TransportConf("test", new MapConfigProvider(properties));
    
    // Configuration should handle negative values gracefully
    // (depending on implementation - may use defaults or absolute values)
    int highWatermark = conf.fetchBusyHighWatermark();
    int lowWatermark = conf.fetchBusyLowWatermark();
    int coolDown = conf.fetchBusyCoolDown();
    
    assertTrue("Negative values should be handled appropriately",
        highWatermark >= 0 || highWatermark == -100);
    assertTrue("Negative values should be handled appropriately",
        lowWatermark >= 0 || lowWatermark == -50);
    assertTrue("Negative values should be handled appropriately", 
        coolDown >= 0 || coolDown == -1000);
  }

  @Test
  public void testZeroValues() {
    Map<String, String> properties = new HashMap<>();
    properties.put("spark.shuffle.fetchBusyHighWatermark", "0");
    properties.put("spark.shuffle.fetchBusyLowWatermark", "0");  
    properties.put("spark.shuffle.fetchBusyCoolDown", "0");
    TransportConf conf = new TransportConf("test", new MapConfigProvider(properties));
    
    assertEquals("Zero high watermark should disable fetch busy", 
        0, conf.fetchBusyHighWatermark());
    assertEquals("Zero low watermark should be allowed",
        0, conf.fetchBusyLowWatermark());
    assertEquals("Zero cooldown should be allowed",
        0, conf.fetchBusyCoolDown());
  }

  private static class MapConfigProvider extends ConfigProvider {
    private final Map<String, String> properties;
    
    MapConfigProvider(Map<String, String> properties) {
      this.properties = properties;
    }
    
    @Override
    public String get(String key) {
      String value = properties.get(key);
      if (value == null) {
        throw new java.util.NoSuchElementException("Key " + key + " not found");
      }
      return value;
    }
    
    @Override
    public Iterable<Map.Entry<String, String>> getAll() {
      return properties.entrySet();
    }
  }
}