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

package org.apache.spark.executor;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class ThreadDumpReporter{



  private static final Logger logger = LoggerFactory.getLogger(ThreadDumpReporter.class);

  private static final Marker THEAD_DUMP_MARKER = MarkerFactory.getMarker("THREAD_DUMP");

  private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private final ScheduledExecutorService executorService = ThreadUtils
      .newDaemonThreadPoolScheduledExecutor(
          "thread-dump-reporter", 1);

  ThreadDumpReporter(long initialDelay, long period){
    logger.info("Stared ThreadDumpReport with initialDelay {}s, period {}s", initialDelay, period);
    executorService.scheduleWithFixedDelay(this::run, initialDelay, period, TimeUnit.SECONDS);
  }
  public void run() {
    logger.info("Capturing thread dump.");

    ThreadInfo[] threadInfo =
        threadMXBean.dumpAllThreads(/* lockedMonitors= */ true, /* lockedSynchronizers= */ true);
    String threadDump =
        Stream.of(threadInfo)
            .filter(Objects::nonNull)
            .map(this::threadInfoToString)
            .collect(Collectors.joining("\n", "\n<THREAD_DUMP_START>", "<THREAD_DUMP_END>\n"));
    logger.info(THEAD_DUMP_MARKER, threadDump);
  }

  // Copied from ThreadInfo.toString() by removing limit of max depth of stack trace (8)
  String threadInfoToString(ThreadInfo threadInfo) {
    StringBuilder sb =
        new StringBuilder(
            "\""
                + threadInfo.getThreadName()
                + "\""
                + (threadInfo.isDaemon() ? " daemon" : "")
                + " prio="
                + threadInfo.getPriority()
                + " Id="
                + threadInfo.getThreadId()
                + " "
                + threadInfo.getThreadState());
    if (threadInfo.getLockName() != null) {
      sb.append(" on " + threadInfo.getLockName());
    }
    if (threadInfo.getLockOwnerName() != null) {
      sb.append(
          " owned by \"" + threadInfo.getLockOwnerName() + "\" Id=" + threadInfo.getLockOwnerId());
    }
    if (threadInfo.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (threadInfo.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');
    int i = 0;
    StackTraceElement[] stackTrace = threadInfo.getStackTrace();
    // Original: private static final int MAX_FRAMES = 8;
    // Original: for (; i < stackTrace.length && i < MAX_FRAMES; i++) {
    for (; i < stackTrace.length; i++) {
      StackTraceElement ste = stackTrace[i];
      sb.append("\tat " + ste.toString());
      sb.append('\n');
      if (i == 0 && threadInfo.getLockInfo() != null) {
        Thread.State ts = threadInfo.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on " + threadInfo.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on " + threadInfo.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on " + threadInfo.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      for (MonitorInfo mi : threadInfo.getLockedMonitors()) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked " + mi);
          sb.append('\n');
        }
      }
    }

    LockInfo[] locks = threadInfo.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = " + locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- " + li);
        sb.append('\n');
      }
    }
    sb.append('\n');
    return sb.toString();
  }
}
