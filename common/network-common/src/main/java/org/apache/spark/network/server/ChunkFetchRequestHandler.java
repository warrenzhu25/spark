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

package org.apache.spark.network.server;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.shuffle.ShuffleFetchMetrics;

import static org.apache.spark.network.util.NettyUtils.*;

/**
 * A dedicated ChannelHandler for processing ChunkFetchRequest messages. When sending response
 * of ChunkFetchRequest messages to the clients, the thread performing the I/O on the underlying
 * channel could potentially be blocked due to disk contentions. If several hundreds of clients
 * send ChunkFetchRequest to the server at the same time, it could potentially occupying all
 * threads from TransportServer's default EventLoopGroup for waiting for disk reads before it
 * can send the block data back to the client as part of the ChunkFetchSuccess messages. As a
 * result, it would leave no threads left to process other RPC messages, which takes much less
 * time to process, and could lead to client timing out on either performing SASL authentication,
 * registering executors, or waiting for response for an OpenBlocks messages.
 */
public class ChunkFetchRequestHandler extends SimpleChannelInboundHandler<ChunkFetchRequest> {
  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(ChunkFetchRequestHandler.class);

  private final TransportClient client;
  private final StreamManager streamManager;
  /** The max number of chunks being transferred and not finished yet. */
  private final long maxChunksBeingTransferred;
  private final boolean syncModeEnabled;
  // Metrics for timing breakdown
  private final Timer chunkFetchLatencyMillis;
  private final Timer chunkReadLatencyMillis;
  private final Timer responseSendLatencyMillis;
  private final Counter chunkFetchQueueDepth;
  private final Timer queueWaitTimeMillis;
  private final Histogram queueLengthHistogram;
  // Per-shuffle metrics tracking
  private final ConcurrentHashMap<Long, Integer> streamToShuffleMap;
  private final ConcurrentHashMap<Integer, Timer> perShuffleLatencyTimers;
  private final ConcurrentHashMap<Integer, Timer> perShuffleReadLatencyTimers;
  private final ConcurrentHashMap<Integer, Timer> perShuffleResponseSendLatencyTimers;

  public ChunkFetchRequestHandler(
      TransportClient client,
      StreamManager streamManager,
      Long maxChunksBeingTransferred,
      boolean syncModeEnabled) {
    this(client, streamManager, maxChunksBeingTransferred, syncModeEnabled,
      null, null, null, null, null, null, null, null, null, null);
  }

  public ChunkFetchRequestHandler(
      TransportClient client,
      StreamManager streamManager,
      Long maxChunksBeingTransferred,
      boolean syncModeEnabled,
      ShuffleFetchMetrics metrics) {
    this(client, streamManager, maxChunksBeingTransferred, syncModeEnabled,
      metrics != null ? metrics.getChunkFetchLatencyMillis() : null,
      metrics != null ? metrics.getChunkReadLatencyMillis() : null,
      metrics != null ? metrics.getResponseSendLatencyMillis() : null,
      metrics != null ? metrics.getChunkFetchQueueDepth() : null,
      metrics != null ? metrics.getQueueWaitTimeMillis() : null,
      metrics != null ? metrics.getQueueLengthHistogram() : null,
      metrics != null ? metrics.getStreamToShuffleMap() : null,
      metrics != null ? metrics.getPerShuffleLatencyTimers() : null,
      metrics != null ? metrics.getPerShuffleReadLatencyTimers() : null,
      metrics != null ? metrics.getPerShuffleResponseSendLatencyTimers() : null);
  }

  public ChunkFetchRequestHandler(
      TransportClient client,
      StreamManager streamManager,
      Long maxChunksBeingTransferred,
      boolean syncModeEnabled,
      Timer chunkFetchLatencyMillis,
      Timer chunkReadLatencyMillis,
      Timer responseSendLatencyMillis,
      Counter chunkFetchQueueDepth,
      Timer queueWaitTimeMillis,
      Histogram queueLengthHistogram,
      ConcurrentHashMap<Long, Integer> streamToShuffleMap,
      ConcurrentHashMap<Integer, Timer> perShuffleLatencyTimers,
      ConcurrentHashMap<Integer, Timer> perShuffleReadLatencyTimers,
      ConcurrentHashMap<Integer, Timer> perShuffleResponseSendLatencyTimers) {
    this.client = client;
    this.streamManager = streamManager;
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    this.syncModeEnabled = syncModeEnabled;
    this.chunkFetchLatencyMillis = chunkFetchLatencyMillis;
    this.chunkReadLatencyMillis = chunkReadLatencyMillis;
    this.responseSendLatencyMillis = responseSendLatencyMillis;
    this.chunkFetchQueueDepth = chunkFetchQueueDepth;
    this.queueWaitTimeMillis = queueWaitTimeMillis;
    this.queueLengthHistogram = queueLengthHistogram;
    this.streamToShuffleMap = streamToShuffleMap;
    this.perShuffleLatencyTimers = perShuffleLatencyTimers;
    this.perShuffleReadLatencyTimers = perShuffleReadLatencyTimers;
    this.perShuffleResponseSendLatencyTimers = perShuffleResponseSendLatencyTimers;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Exception in connection from {}", cause,
      MDC.of(LogKeys.HOST_PORT, getRemoteAddress(ctx.channel())));
    ctx.close();
  }

  /**
   * Records latency in the per-shuffle timer if per-shuffle tracking is enabled.
   */
  private void recordPerShuffleLatency(
      long streamId,
      long durationNanos,
      ConcurrentHashMap<Integer, Timer> timers) {
    if (streamToShuffleMap != null && timers != null) {
      Integer shuffleId = streamToShuffleMap.get(streamId);
      if (shuffleId != null) {
        Timer perShuffleTimer = timers.computeIfAbsent(shuffleId, id -> new Timer());
        perShuffleTimer.update(durationNanos, TimeUnit.NANOSECONDS);
      }
    }
  }

  @Override
  protected void channelRead0(
      ChannelHandlerContext ctx,
      final ChunkFetchRequest msg) throws Exception {
    // Measure queue wait time (from arrival to processing start)
    final long processingStartNanos = System.nanoTime();
    final long arrivalTimeNanos = msg.getArrivalTimeNanos();
    if (queueWaitTimeMillis != null && arrivalTimeNanos > 0) {
      final long queueWaitNanos = processingStartNanos - arrivalTimeNanos;
      queueWaitTimeMillis.update(queueWaitNanos, TimeUnit.NANOSECONDS);
    }

    Channel channel = ctx.channel();
    processFetchRequest(channel, msg);
  }

  public void processFetchRequest(
      final Channel channel, final ChunkFetchRequest msg) throws Exception {
    // Start timing total request processing
    long requestStartNanos = System.nanoTime();

    // Increment queue depth (tracks concurrent requests)
    if (chunkFetchQueueDepth != null) {
      chunkFetchQueueDepth.inc();
    }

    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
        msg.streamChunkId);
    }
    if (maxChunksBeingTransferred < Long.MAX_VALUE) {
      long chunksBeingTransferred = streamManager.chunksBeingTransferred();
      if (chunksBeingTransferred >= maxChunksBeingTransferred) {
        logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
          MDC.of(LogKeys.NUM_CHUNKS, chunksBeingTransferred),
          MDC.of(LogKeys.MAX_NUM_CHUNKS, maxChunksBeingTransferred));
        channel.close();
        // Decrement queue depth since request won't be processed
        if (chunkFetchQueueDepth != null) {
          chunkFetchQueueDepth.dec();
        }
        return;
      }
    }
    ManagedBuffer buf;
    try {
      streamManager.checkAuthorization(client, msg.streamChunkId.streamId());

      // Time getChunk (disk I/O) separately for breakdown
      long getChunkStartNanos = System.nanoTime();
      buf = streamManager.getChunk(msg.streamChunkId.streamId(), msg.streamChunkId.chunkIndex());
      long getChunkDurationNanos = System.nanoTime() - getChunkStartNanos;

      if (chunkReadLatencyMillis != null) {
        chunkReadLatencyMillis.update(getChunkDurationNanos, TimeUnit.NANOSECONDS);
        recordPerShuffleLatency(msg.streamChunkId.streamId(), getChunkDurationNanos,
          perShuffleReadLatencyTimers);
      }

      if (buf == null) {
        throw new IllegalStateException("Chunk was not found");
      }
    } catch (Exception e) {
      logger.error("Error opening block {} for request from {}", e,
        MDC.of(LogKeys.STREAM_CHUNK_ID, msg.streamChunkId),
        MDC.of(LogKeys.HOST_PORT, getRemoteAddress(channel)));
      long respondStartNanos = System.nanoTime();
      respond(channel, new ChunkFetchFailure(msg.streamChunkId,
        JavaUtils.stackTraceToString(e))).addListener((ChannelFutureListener) future -> {
          if (responseSendLatencyMillis != null) {
            long respondDurationNanos = System.nanoTime() - respondStartNanos;
            responseSendLatencyMillis.update(respondDurationNanos, TimeUnit.NANOSECONDS);
            recordPerShuffleLatency(msg.streamChunkId.streamId(), respondDurationNanos,
              perShuffleResponseSendLatencyTimers);
          }
          if (chunkFetchLatencyMillis != null) {
            long totalDuration = System.nanoTime() - requestStartNanos;
            chunkFetchLatencyMillis.update(totalDuration, TimeUnit.NANOSECONDS);
            // Also record in per-shuffle timer
            recordPerShuffleLatency(msg.streamChunkId.streamId(), totalDuration,
              perShuffleLatencyTimers);
          }
          // Decrement queue depth after request completes
          if (chunkFetchQueueDepth != null) {
            chunkFetchQueueDepth.dec();
          }
        });
      return;
    }

    streamManager.chunkBeingSent(msg.streamChunkId.streamId());

    // Time respond operation separately for breakdown
    long respondStartNanos = System.nanoTime();
    respond(channel, new ChunkFetchSuccess(msg.streamChunkId, buf)).addListener(
      (ChannelFutureListener) future -> {
        streamManager.chunkSent(msg.streamChunkId.streamId());

        // Update respond time after response is sent
        if (responseSendLatencyMillis != null) {
          long respondDurationNanos = System.nanoTime() - respondStartNanos;
          responseSendLatencyMillis.update(respondDurationNanos, TimeUnit.NANOSECONDS);
          recordPerShuffleLatency(msg.streamChunkId.streamId(), respondDurationNanos,
            perShuffleResponseSendLatencyTimers);
        }

        // Update total processing time (used for wait estimation)
        if (chunkFetchLatencyMillis != null) {
          long totalDuration = System.nanoTime() - requestStartNanos;
          chunkFetchLatencyMillis.update(totalDuration, TimeUnit.NANOSECONDS);
          // Also record in per-shuffle timer
          recordPerShuffleLatency(msg.streamChunkId.streamId(), totalDuration,
            perShuffleLatencyTimers);
        }

        // Decrement queue depth after request completes
        if (chunkFetchQueueDepth != null) {
          chunkFetchQueueDepth.dec();
        }
      });
  }

  /**
   * The invocation to channel.writeAndFlush is async, and the actual I/O on the
   * channel will be handled by the EventLoop the channel is registered to. So even
   * though we are processing the ChunkFetchRequest in a separate thread pool, the actual I/O,
   * which is the potentially blocking call that could deplete server handler threads, is still
   * being processed by TransportServer's default EventLoopGroup.
   *
   * When syncModeEnabled is true, Spark will throttle the max number of threads that channel I/O
   * for sending response to ChunkFetchRequest, the thread calling channel.writeAndFlush will wait
   * for the completion of sending response back to client by invoking await(). This will throttle
   * the rate at which threads from ChunkFetchRequest dedicated EventLoopGroup submit channel I/O
   * requests to TransportServer's default EventLoopGroup, thus making sure that we can reserve
   * some threads in TransportServer's default EventLoopGroup for handling other RPC messages.
   */
  private ChannelFuture respond(
      final Channel channel,
      final Encodable result) throws InterruptedException {
    final SocketAddress remoteAddress = channel.remoteAddress();
    ChannelFuture channelFuture;
    if (syncModeEnabled) {
      channelFuture = channel.writeAndFlush(result).await();
    } else {
      channelFuture = channel.writeAndFlush(result);
    }
    return channelFuture.addListener((ChannelFutureListener) future -> {
      if (future.isSuccess()) {
        logger.trace("Sent result {} to client {}", result, remoteAddress);
      } else {
        logger.error("Error sending result {} to {}; closing connection",
          future.cause(),
          MDC.of(LogKeys.RESULT, result),
          MDC.of(LogKeys.HOST_PORT, remoteAddress));
        channel.close();
      }
    });
  }
}
