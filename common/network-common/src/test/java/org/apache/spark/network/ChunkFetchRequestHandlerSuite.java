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

import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;

public class ChunkFetchRequestHandlerSuite {

  @Test
  public void testServerMetricsCollection() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenReturn(channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
      Object response = invocation.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
      return channelFuture;
    });

    // Prepare stream with test buffers (TestManagedBuffer max size is 127)
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(50));
    managedBuffers.add(new TestManagedBuffer(100));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
        reverseClient, rpcHandler.getStreamManager(), Long.MAX_VALUE, false);

    // Verify initial metrics are zero
    ChunkFetchRequestHandler.ServerMetrics initialMetrics = requestHandler.getServerMetrics();
    Assert.assertEquals(0, initialMetrics.totalRequestsReceived);
    Assert.assertEquals(0, initialMetrics.totalRequestsCompleted);
    Assert.assertEquals(0, initialMetrics.totalRequestsFailed);
    Assert.assertEquals(0, initialMetrics.totalBytesServed);
    Assert.assertEquals(0, initialMetrics.currentQueueDepth);

    // Process successful request
    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request1);

    // Complete the request
    responseAndPromisePairs.get(0).getRight().finish(true);

    // Verify metrics after successful request
    ChunkFetchRequestHandler.ServerMetrics metrics1 = requestHandler.getServerMetrics();
    Assert.assertEquals(1, metrics1.totalRequestsReceived);
    Assert.assertEquals(1, metrics1.totalRequestsCompleted);
    Assert.assertEquals(0, metrics1.totalRequestsFailed);
    Assert.assertEquals(50, metrics1.totalBytesServed);
    Assert.assertEquals(0, metrics1.currentQueueDepth);
    Assert.assertTrue("Expected positive processing time, got: " + metrics1.getAvgProcessingTimeMs(), 
      metrics1.getAvgProcessingTimeMs() >= 0);
    Assert.assertEquals(1.0, metrics1.getSuccessRate(), 0.01);

    // Process another successful request
    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.channelRead(context, request2);
    responseAndPromisePairs.get(1).getRight().finish(true);

    // Verify accumulated metrics
    ChunkFetchRequestHandler.ServerMetrics metrics2 = requestHandler.getServerMetrics();
    Assert.assertEquals(2, metrics2.totalRequestsReceived);
    Assert.assertEquals(2, metrics2.totalRequestsCompleted);
    Assert.assertEquals(0, metrics2.totalRequestsFailed);
    Assert.assertEquals(150, metrics2.totalBytesServed); // 50 + 100
    Assert.assertEquals(1.0, metrics2.getSuccessRate(), 0.01);

    // Test metrics reset
    requestHandler.resetMetrics();
    ChunkFetchRequestHandler.ServerMetrics resetMetrics = requestHandler.getServerMetrics();
    Assert.assertEquals(0, resetMetrics.totalRequestsReceived);
    Assert.assertEquals(0, resetMetrics.totalRequestsCompleted);
    Assert.assertEquals(0, resetMetrics.totalBytesServed);
  }

  @Test
  public void testServerMetricsWithFailures() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenReturn(channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
      Object response = invocation.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
      return channelFuture;
    });

    // Prepare stream with null buffer to trigger failure
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(null); // This will cause a failure
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);

    TransportClient reverseClient = mock(TransportClient.class);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(
        reverseClient, rpcHandler.getStreamManager(), Long.MAX_VALUE, false);

    // Process request that will fail
    RequestMessage failingRequest = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, failingRequest);

    // Verify failure metrics
    ChunkFetchRequestHandler.ServerMetrics metrics = requestHandler.getServerMetrics();
    Assert.assertEquals(1, metrics.totalRequestsReceived);
    Assert.assertEquals(0, metrics.totalRequestsCompleted);
    Assert.assertEquals(1, metrics.totalRequestsFailed);
    Assert.assertEquals(0, metrics.totalBytesServed);
    Assert.assertEquals(0.0, metrics.getSuccessRate(), 0.01);
  }

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
        responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
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
    Assert.assertEquals(1, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);
    Assert.assertEquals(managedBuffers.get(0),
      ((ChunkFetchSuccess) (responseAndPromisePairs.get(0).getLeft())).body());

    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.channelRead(context, request1);
    Assert.assertEquals(2, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(1).getLeft() instanceof ChunkFetchSuccess);
    Assert.assertEquals(managedBuffers.get(1),
      ((ChunkFetchSuccess) (responseAndPromisePairs.get(1).getLeft())).body());

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 2));
    requestHandler.channelRead(context, request2);
    Assert.assertEquals(3, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(2).getLeft() instanceof ChunkFetchFailure);
    ChunkFetchFailure chunkFetchFailure =
        ((ChunkFetchFailure) (responseAndPromisePairs.get(2).getLeft()));
    Assert.assertEquals("java.lang.IllegalStateException: Chunk was not found",
        chunkFetchFailure.errorString.split("\\r?\\n")[0]);

    RequestMessage request3 = new ChunkFetchRequest(new StreamChunkId(streamId, 3));
    requestHandler.channelRead(context, request3);
    Assert.assertEquals(4, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(3).getLeft() instanceof ChunkFetchSuccess);
    Assert.assertEquals(managedBuffers.get(3),
      ((ChunkFetchSuccess) (responseAndPromisePairs.get(3).getLeft())).body());

    RequestMessage request4 = new ChunkFetchRequest(new StreamChunkId(streamId, 4));
    requestHandler.channelRead(context, request4);
    verify(channel, times(1)).close();
    Assert.assertEquals(4, responseAndPromisePairs.size());
  }
}
