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

package org.apache.spark.network;

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import io.netty.channel.Channel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.shuffle.ShuffleFetchMetrics;
import org.apache.spark.util.Pair;

public class ChunkFetchRequestHandlerSuite {

  @Test
  public void handleChunkFetchRequest() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel())
      .thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs =
      new ArrayList<>();
    when(channel.writeAndFlush(any()))
      .thenAnswer(invocationOnMock0 -> {
        Object response = invocationOnMock0.getArguments()[0];
        ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
        responseAndPromisePairs.add(Pair.of(response, channelFuture));
        return channelFuture;
      });

    // Prepare the stream.
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    managedBuffers.add(null);
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);
    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
      rpcHandler.getStreamManager(), 2L, false);

    RequestMessage request0 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request0);
    Assertions.assertEquals(1, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);
    Assertions.assertEquals(managedBuffers.get(0),
      ((ChunkFetchSuccess) (responseAndPromisePairs.get(0).getLeft())).body());

    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.channelRead(context, request1);
    Assertions.assertEquals(2, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(1).getLeft() instanceof ChunkFetchSuccess);
    Assertions.assertEquals(managedBuffers.get(1),
      ((ChunkFetchSuccess) (responseAndPromisePairs.get(1).getLeft())).body());

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 2));
    requestHandler.channelRead(context, request2);
    Assertions.assertEquals(3, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(2).getLeft() instanceof ChunkFetchFailure);
    ChunkFetchFailure chunkFetchFailure =
        ((ChunkFetchFailure) (responseAndPromisePairs.get(2).getLeft()));
    Assertions.assertEquals("java.lang.IllegalStateException: Chunk was not found",
        chunkFetchFailure.errorString.split("\\r?\\n")[0]);

    RequestMessage request3 = new ChunkFetchRequest(new StreamChunkId(streamId, 3));
    requestHandler.channelRead(context, request3);
    Assertions.assertEquals(4, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(3).getLeft() instanceof ChunkFetchSuccess);
    Assertions.assertEquals(managedBuffers.get(3),
      ((ChunkFetchSuccess) (responseAndPromisePairs.get(3).getLeft())).body());

    RequestMessage request4 = new ChunkFetchRequest(new StreamChunkId(streamId, 4));
    requestHandler.channelRead(context, request4);
    verify(channel, times(1)).close();
    Assertions.assertEquals(4, responseAndPromisePairs.size());
  }

  @Test
  public void testMetricsRecordedOnSuccess() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(Pair.of(response, channelFuture));
      return channelFuture;
    });

    // Create metrics
    Timer chunkFetchLatencyMillis = new Timer();
    Timer chunkReadLatencyMillis = new Timer();
    Timer responseSendLatencyMillis = new Timer();
    Counter queueDepth = new Counter();
    ShuffleFetchMetrics metrics = new ShuffleFetchMetrics(
      chunkFetchLatencyMillis, chunkReadLatencyMillis, responseSendLatencyMillis, queueDepth,
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>());

    // Prepare the stream with one buffer
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
      reverseClient, rpcHandler.getStreamManager(), 2L, false, metrics);

    // Fetch the chunk
    RequestMessage request = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request);

    // Verify response
    Assertions.assertEquals(1, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);

    // Complete the channel future to trigger metric updates
    responseAndPromisePairs.get(0).getRight().finish(true);

    // Verify all metrics were updated
    Assertions.assertEquals(1, chunkFetchLatencyMillis.getCount(),
      "Total fetch latency should be recorded");
    Assertions.assertEquals(1, chunkReadLatencyMillis.getCount(),
      "Disk read latency should be recorded");
    Assertions.assertEquals(1, responseSendLatencyMillis.getCount(),
      "Response send latency should be recorded");

    // Verify timing values are reasonable (non-zero)
    Assertions.assertTrue(chunkFetchLatencyMillis.getSnapshot().getMax() > 0,
      "Total fetch time should be positive");
    Assertions.assertTrue(chunkReadLatencyMillis.getSnapshot().getMax() >= 0,
      "Disk read time should be non-negative");
    Assertions.assertTrue(responseSendLatencyMillis.getSnapshot().getMax() >= 0,
      "Response send time should be non-negative");
  }

  @Test
  public void testMetricsRecordedOnFailure() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(Pair.of(response, channelFuture));
      return channelFuture;
    });

    // Create metrics
    Timer chunkFetchLatencyMillis = new Timer();
    Timer chunkReadLatencyMillis = new Timer();
    Timer responseSendLatencyMillis = new Timer();
    Counter queueDepth = new Counter();
    ShuffleFetchMetrics metrics = new ShuffleFetchMetrics(
      chunkFetchLatencyMillis, chunkReadLatencyMillis, responseSendLatencyMillis, queueDepth,
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>());

    // Prepare the stream with a null buffer (will cause failure)
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(null);
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
      reverseClient, rpcHandler.getStreamManager(), 2L, false, metrics);

    // Fetch the chunk (should fail)
    RequestMessage request = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request);

    // Verify failure response
    Assertions.assertEquals(1, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchFailure);

    // Complete the channel future to trigger metric updates
    responseAndPromisePairs.get(0).getRight().finish(true);

    // Verify metrics were updated even on failure
    Assertions.assertEquals(1, chunkFetchLatencyMillis.getCount(),
      "Total fetch latency should be recorded on failure");
    Assertions.assertEquals(1, chunkReadLatencyMillis.getCount(),
      "Disk read latency should be recorded (measures getChunk call time, even when null)");
    Assertions.assertEquals(1, responseSendLatencyMillis.getCount(),
      "Response send latency should be recorded on failure");
  }

  @Test
  public void testNullMetricsHandledGracefully() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(Pair.of(response, channelFuture));
      return channelFuture;
    });

    // Prepare the stream with one buffer
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    // Create handler with null metrics (no ShuffleFetchMetrics passed)
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
      reverseClient, rpcHandler.getStreamManager(), 2L, false);

    // Fetch the chunk - should succeed without throwing exception
    RequestMessage request = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request);

    // Verify response
    Assertions.assertEquals(1, responseAndPromisePairs.size());
    Assertions.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);

    // Complete the channel future - should not throw exception
    responseAndPromisePairs.get(0).getRight().finish(true);

    // Test passed if no exceptions thrown
  }

  @Test
  public void testMetricsBreakdownRelationship() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(Pair.of(response, channelFuture));
      return channelFuture;
    });

    // Create metrics
    Timer chunkFetchLatencyMillis = new Timer();
    Timer chunkReadLatencyMillis = new Timer();
    Timer responseSendLatencyMillis = new Timer();
    Counter queueDepth = new Counter();
    ShuffleFetchMetrics metrics = new ShuffleFetchMetrics(
      chunkFetchLatencyMillis, chunkReadLatencyMillis, responseSendLatencyMillis, queueDepth,
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>());

    // Prepare the stream with one buffer
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
      reverseClient, rpcHandler.getStreamManager(), 2L, false, metrics);

    // Fetch the chunk
    RequestMessage request = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request);
    responseAndPromisePairs.get(0).getRight().finish(true);

    // Verify that total time >= disk read time + response send time
    // (May not be exactly equal due to other overhead like authorization, serialization, etc.)
    long totalTimeNanos = chunkFetchLatencyMillis.getSnapshot().getMax();
    long diskReadTimeNanos = chunkReadLatencyMillis.getSnapshot().getMax();
    long responseSendTimeNanos = responseSendLatencyMillis.getSnapshot().getMax();

    Assertions.assertTrue(totalTimeNanos >= diskReadTimeNanos,
      "Total time should be >= disk read time");
    Assertions.assertTrue(totalTimeNanos >= responseSendTimeNanos,
      "Total time should be >= response send time");
    // Note: We don't assert total == disk + response because there's overhead
    // (authorization, serialization, etc.) that's part of total but not in breakdown metrics
  }

  @Test
  public void testQueueDepthTracking() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(Pair.of(response, channelFuture));
      return channelFuture;
    });

    // Create metrics with queue depth counter
    Timer chunkFetchLatencyMillis = new Timer();
    Timer chunkReadLatencyMillis = new Timer();
    Timer responseSendLatencyMillis = new Timer();
    Counter queueDepth = new Counter();
    ShuffleFetchMetrics metrics = new ShuffleFetchMetrics(
      chunkFetchLatencyMillis, chunkReadLatencyMillis, responseSendLatencyMillis, queueDepth,
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>(),
      new ConcurrentHashMap<>(), new ConcurrentHashMap<>());

    // Prepare the stream with buffers
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
      reverseClient, rpcHandler.getStreamManager(), 2L, false, metrics);

    // Initially queue depth should be 0
    Assertions.assertEquals(0, queueDepth.getCount(), "Initial queue depth should be 0");

    // Fetch first chunk - queue depth should increase to 1
    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request1);
    Assertions.assertEquals(1, queueDepth.getCount(),
      "Queue depth should be 1 while request is in flight");

    // Fetch second chunk before completing first - queue depth should be 2
    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.channelRead(context, request2);
    Assertions.assertEquals(2, queueDepth.getCount(),
      "Queue depth should be 2 with two requests in flight");

    // Complete first request - queue depth should decrease to 1
    responseAndPromisePairs.get(0).getRight().finish(true);
    Assertions.assertEquals(1, queueDepth.getCount(),
      "Queue depth should be 1 after completing one request");

    // Complete second request - queue depth should return to 0
    responseAndPromisePairs.get(1).getRight().finish(true);
    Assertions.assertEquals(0, queueDepth.getCount(),
      "Queue depth should be 0 after completing all requests");
  }

  @Test
  public void testPerShuffleMetricsTracking() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) rpcHandler.getStreamManager();
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenReturn(channel);
    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(Pair.of(response, channelFuture));
      return channelFuture;
    });

    // Create metrics with per-shuffle tracking enabled
    Timer chunkFetchLatencyMillis = new Timer();
    Timer chunkReadLatencyMillis = new Timer();
    Timer responseSendLatencyMillis = new Timer();
    Counter queueDepth = new Counter();
    ConcurrentHashMap<Long, Integer> streamToShuffleMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Timer> perShuffleLatencyTimers = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Timer> perShuffleReadLatencyTimers = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, Timer> perShuffleResponseSendLatencyTimers = new ConcurrentHashMap<>();
    ShuffleFetchMetrics metrics = new ShuffleFetchMetrics(
      chunkFetchLatencyMillis, chunkReadLatencyMillis, responseSendLatencyMillis, queueDepth,
      streamToShuffleMap, perShuffleLatencyTimers, perShuffleReadLatencyTimers,
      perShuffleResponseSendLatencyTimers);

    // Prepare two streams for different shuffles
    List<ManagedBuffer> managedBuffers1 = new ArrayList<>();
    managedBuffers1.add(new TestManagedBuffer(10));
    long streamId1 = streamManager.registerStream("test-app", managedBuffers1.iterator(), channel);
    streamToShuffleMap.put(streamId1, 100); // Shuffle ID 100

    List<ManagedBuffer> managedBuffers2 = new ArrayList<>();
    managedBuffers2.add(new TestManagedBuffer(20));
    long streamId2 = streamManager.registerStream("test-app", managedBuffers2.iterator(), channel);
    streamToShuffleMap.put(streamId2, 200); // Shuffle ID 200

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
      reverseClient, streamManager, 1000L, false, metrics);

    // Fetch from shuffle 100
    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId1, 0));
    requestHandler.channelRead(context, request1);
    responseAndPromisePairs.get(0).getRight().finish(true);

    // Fetch from shuffle 200
    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId2, 0));
    requestHandler.channelRead(context, request2);
    responseAndPromisePairs.get(1).getRight().finish(true);

    // Verify global metrics recorded both requests
    Assertions.assertEquals(2, chunkFetchLatencyMillis.getCount(),
      "Global metrics should record all requests");

    // Verify per-shuffle metrics exist
    Assertions.assertTrue(perShuffleLatencyTimers.containsKey(100),
      "Per-shuffle timer should exist for shuffle 100");
    Assertions.assertTrue(perShuffleLatencyTimers.containsKey(200),
      "Per-shuffle timer should exist for shuffle 200");
    Assertions.assertTrue(perShuffleReadLatencyTimers.containsKey(100),
      "Per-shuffle read timer should exist for shuffle 100");
    Assertions.assertTrue(perShuffleReadLatencyTimers.containsKey(200),
      "Per-shuffle read timer should exist for shuffle 200");
    Assertions.assertTrue(perShuffleResponseSendLatencyTimers.containsKey(100),
      "Per-shuffle response timer should exist for shuffle 100");
    Assertions.assertTrue(perShuffleResponseSendLatencyTimers.containsKey(200),
      "Per-shuffle response timer should exist for shuffle 200");

    // Verify per-shuffle metrics recorded one request each
    Assertions.assertEquals(1, perShuffleLatencyTimers.get(100).getCount(),
      "Per-shuffle timer for shuffle 100 should record 1 request");
    Assertions.assertEquals(1, perShuffleLatencyTimers.get(200).getCount(),
      "Per-shuffle timer for shuffle 200 should record 1 request");
    Assertions.assertEquals(1, perShuffleReadLatencyTimers.get(100).getCount(),
      "Per-shuffle read timer for shuffle 100 should record 1 request");
    Assertions.assertEquals(1, perShuffleReadLatencyTimers.get(200).getCount(),
      "Per-shuffle read timer for shuffle 200 should record 1 request");
    Assertions.assertEquals(1, perShuffleResponseSendLatencyTimers.get(100).getCount(),
      "Per-shuffle response timer for shuffle 100 should record 1 request");
    Assertions.assertEquals(1, perShuffleResponseSendLatencyTimers.get(200).getCount(),
      "Per-shuffle response timer for shuffle 200 should record 1 request");
  }
}
