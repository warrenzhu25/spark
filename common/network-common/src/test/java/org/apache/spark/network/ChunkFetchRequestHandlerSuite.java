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
import org.apache.spark.network.server.FetchBusyChecker;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

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
    FetchBusyChecker fetchBusyChecker = new FetchBusyChecker(5, 1, 1);
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
      rpcHandler.getStreamManager(), 2L, false, fetchBusyChecker);

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

  @Test
  public void handleChunkFetchRequestWithFetchBusyChecker() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
      return channelFuture;
    });

    // Prepare the stream
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);
    
    TransportClient reverseClient = mock(TransportClient.class);
    
    // Create a fetch busy checker that tracks latency
    FetchBusyChecker busyChecker = mock(FetchBusyChecker.class);
    when(busyChecker.isFetchBusy()).thenReturn(true);
    
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
      rpcHandler.getStreamManager(), 2L, false, busyChecker);

    RequestMessage request = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request);
    
    // Should process the request normally (current implementation doesn't reject based on fetch busy)
    // but should call handle() on the fetch busy checker to track latency
    Assert.assertEquals(1, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);
    
    // Verify that the fetch busy checker was called to handle latency
    verify(busyChecker, times(1)).handle(anyLong());
    
    // Verify that isFetchBusy method works correctly
    Assert.assertTrue("ChunkFetchRequestHandler should report fetch busy state", 
        requestHandler.isFetchBusy());
  }

  @Test
  public void handleChunkFetchRequestWithoutFetchBusyChecker() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
      return channelFuture;
    });

    // Prepare the stream
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);
    
    TransportClient reverseClient = mock(TransportClient.class);
    
    // Create handler without fetch busy checker
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
      rpcHandler.getStreamManager(), 2L, false);

    RequestMessage request = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.channelRead(context, request);
    
    // Should process the request normally and not report fetch busy
    Assert.assertEquals(1, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);
    ChunkFetchSuccess success = (ChunkFetchSuccess) responseAndPromisePairs.get(0).getLeft();
    Assert.assertEquals(managedBuffers.get(0), success.body());
    
    // Should report not fetch busy when no checker is provided
    Assert.assertFalse("ChunkFetchRequestHandler should not report fetch busy without checker", 
        requestHandler.isFetchBusy());
  }

  @Test
  public void handleChunkFetchRequestVerifyBusyStateReporting() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = mock(Channel.class);
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.channel()).thenAnswer(invocationOnMock0 -> channel);

    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs = new ArrayList<>();
    when(channel.writeAndFlush(any())).thenAnswer(invocationOnMock0 -> {
      Object response = invocationOnMock0.getArguments()[0];
      ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
      responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
      return channelFuture;
    });

    // Prepare the stream
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    long streamId = streamManager.registerStream("test-app", managedBuffers.iterator(), channel);
    
    TransportClient reverseClient = mock(TransportClient.class);
    
    // Create a fetch busy checker that transitions from busy to not busy
    FetchBusyChecker transitioningChecker = mock(FetchBusyChecker.class);
    when(transitioningChecker.isFetchBusy())
      .thenReturn(true)   // First call: busy
      .thenReturn(false); // Second call: not busy
    
    ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
      rpcHandler.getStreamManager(), 2L, false, transitioningChecker);

    // First request should succeed but handler should report busy
    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    Assert.assertTrue("Should report fetch busy initially", requestHandler.isFetchBusy());
    requestHandler.channelRead(context, request1);
    Assert.assertEquals(1, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess);
    verify(transitioningChecker, times(1)).handle(anyLong());

    // Second request should succeed and handler should report not busy
    RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    Assert.assertFalse("Should report not fetch busy after transition", requestHandler.isFetchBusy());
    requestHandler.channelRead(context, request2);
    Assert.assertEquals(2, responseAndPromisePairs.size());
    Assert.assertTrue(responseAndPromisePairs.get(1).getLeft() instanceof ChunkFetchSuccess);
    verify(transitioningChecker, times(2)).handle(anyLong());
  }
}
