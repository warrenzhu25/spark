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

package org.apache.spark.util

import java.io._
import java.lang.{Byte => JByte}
import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, PlatformManagedObject, ThreadInfo}
import java.lang.reflect.InvocationTargetException
import java.math.{MathContext, RoundingMode}
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, FileChannel, WritableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.SecureRandom
import java.util.{Locale, Properties, Random, UUID}
import java.util.concurrent._
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.zip.{GZIPInputStream, ZipInputStream}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.matching.Regex

import _root_.io.netty.channel.unix.Errors.NativeIoException
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Interners
import com.google.common.io.{ByteStreams, Files => GFiles}
import com.google.common.net.InetAddresses
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.ipc.{CallerContext => HadoopCallerContext}
import org.apache.hadoop.ipc.CallerContext.{Builder => HadoopCallerContextBuilder}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.{RunJar, StringUtils}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.eclipse.jetty.util.MultiException
import org.slf4j.Logger

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Streaming._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
import org.apache.spark.util.collection.{Utils => CUtils}
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/** CallSite represents a place in user code. It can have a short and a long form. */
private[spark] case class CallSite(shortForm: String, longForm: String)

private[spark] object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
  val empty = CallSite("", "")
}

/**
 * Various utility methods used by Spark.
 */
private[spark] object Utils
  extends Logging
  with SparkClassUtils
  with SparkErrorUtils
  with SparkFileUtils
  with SparkSerDeUtils {

  private val sparkUncaughtExceptionHandler = new SparkUncaughtExceptionHandler
  @volatile private var cachedLocalDir: String = ""

  /**
   * Define a default value for driver memory here since this value is referenced across the code
   * base and nearly all files already use Utils.scala
   */
  val DEFAULT_DRIVER_MEM_MB = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  @volatile private var localRootDirs: Array[String] = null

  /** Scheme used for files that are locally available on worker nodes in the cluster. */
  val LOCAL_SCHEME = "local"

  private val weakStringInterner = Interners.newWeakInterner[String]()

  private val PATTERN_FOR_COMMAND_LINE_ARG = "-D(.+?)=(.+)".r

  private val COPY_BUFFER_LEN = 1024

  private val copyBuffer = ThreadLocal.withInitial[Array[Byte]](() => {
    new Array[Byte](COPY_BUFFER_LEN)
  })
  /** Deserialize a Long value (used for [[org.apache.spark.api.python.PythonPartitioner]]) */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /** Serialize via nested stream using specific serializer */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /** Deserialize via nested stream using specific serializer */
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /** String interning to reduce the memory usage. */
  def weakIntern(s: String): String = {
    weakStringInterner.intern(s)
  }

  /**
   * Run a segment of code using a different context class loader in the current thread
   */
  def withContextClassLoader[T](ctxClassLoader: ClassLoader)(fn: => T): T = {
    val oldClassLoader = Thread.currentThread().getContextClassLoader()
    try {
      Thread.currentThread().setContextClassLoader(ctxClassLoader)
      fn
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader)
    }
  }

  private def writeByteBufferImpl(bb: ByteBuffer, writer: (Array[Byte], Int, Int) => Unit): Unit = {
    if (bb.hasArray) {
      // Avoid extra copy if the bytebuffer is backed by bytes array
      writer(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      // Fallback to copy approach
      val buffer = {
        // reuse the copy buffer from thread local
        copyBuffer.get()
      }
      val originalPosition = bb.position()
      var bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      while (bytesToCopy > 0) {
        bb.get(buffer, 0, bytesToCopy)
        writer(buffer, 0, bytesToCopy)
        bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      }
      bb.position(originalPosition)
    }
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.DataOutput]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: DataOutput): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.OutputStream]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: OutputStream): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  /**
   * JDK equivalent of `chmod 700 file`.
   *
   * @param file the file whose permissions will be modified
   * @return true if the permissions were successfully changed, false otherwise.
   */
  def chmod700(file: File): Boolean = {
    file.setReadable(false, false) &&
    file.setReadable(true, true) &&
    file.setWritable(false, false) &&
    file.setWritable(true, true) &&
    file.setExecutable(false, false) &&
    file.setExecutable(true, true)
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  override def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
   * Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
   * copying is disabled by default unless explicitly set transferToEnabled as true,
   * the parameter transferToEnabled should be configured by spark.file.transferTo = [true|false].
   */
  def copyStream(
      in: InputStream,
      out: OutputStream,
      closeStreams: Boolean = false,
      transferToEnabled: Boolean = false): Long = {
    tryWithSafeFinally {
      (in, out) match {
        case (input: FileInputStream, output: FileOutputStream) if transferToEnabled =>
          // When both streams are File stream, use transferTo to improve copy performance.
          val inChannel = input.getChannel
          val outChannel = output.getChannel
          val size = inChannel.size()
          copyFileStreamNIO(inChannel, outChannel, 0, size)
          size
        case (input, output) =>
          var count = 0L
          val buf = new Array[Byte](8192)
          var n = 0
          while (n != -1) {
            n = input.read(buf)
            if (n != -1) {
              output.write(buf, 0, n)
              count += n
            }
          }
          count
      }
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  /**
   * Copy the first `maxSize` bytes of data from the InputStream to an in-memory
   * buffer, primarily to check for corruption.
   *
   * This returns a new InputStream which contains the same data as the original input stream.
   * It may be entirely on in-memory buffer, or it may be a combination of in-memory data, and then
   * continue to read from the original stream. The only real use of this is if the original input
   * stream will potentially detect corruption while the data is being read (e.g. from compression).
   * This allows for an eager check of corruption in the first maxSize bytes of data.
   *
   * @return An InputStream which includes all data from the original stream (combining buffered
   *         data and remaining data in the original stream)
   */
  def copyStreamUpTo(in: InputStream, maxSize: Long): InputStream = {
    var count = 0L
    val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
    val fullyCopied = tryWithSafeFinally {
      val bufSize = Math.min(8192L, maxSize)
      val buf = new Array[Byte](bufSize.toInt)
      var n = 0
      while (n != -1 && count < maxSize) {
        n = in.read(buf, 0, Math.min(maxSize - count, bufSize).toInt)
        if (n != -1) {
          out.write(buf, 0, n)
          count += n
        }
      }
      count < maxSize
    } {
      try {
        if (count < maxSize) {
          in.close()
        }
      } finally {
        out.close()
      }
    }
    if (fullyCopied) {
      out.toChunkedByteBuffer.toInputStream(dispose = true)
    } else {
      new SequenceInputStream( out.toChunkedByteBuffer.toInputStream(dispose = true), in)
    }
  }

  def copyFileStreamNIO(
      input: FileChannel,
      output: WritableByteChannel,
      startPosition: Long,
      bytesToCopy: Long): Unit = {
    val outputInitialState = output match {
      case outputFileChannel: FileChannel =>
        Some((outputFileChannel.position(), outputFileChannel))
      case _ => None
    }
    var count = 0L
    // In case transferTo method transferred less data than we have required.
    while (count < bytesToCopy) {
      count += input.transferTo(count + startPosition, bytesToCopy - count, output)
    }
    assert(count == bytesToCopy,
      s"request to copy $bytesToCopy bytes, but actually copied $count bytes.")

    // Check the position after transferTo loop to see if it is in the right position and
    // give user information if not.
    // Position will not be increased to the expected length after calling transferTo in
    // kernel version 2.6.32, this issue can be seen in
    // https://bugs.openjdk.java.net/browse/JDK-7052359
    // This will lead to stream corruption issue when using sort-based shuffle (SPARK-3948).
    outputInitialState.foreach { case (initialPos, outputFileChannel) =>
      val finalPos = outputFileChannel.position()
      val expectedPos = initialPos + bytesToCopy
      assert(finalPos == expectedPos,
        s"""
           |Current position $finalPos do not equal to expected position $expectedPos
           |after transferTo, please check your kernel version to see if it is 2.6.32,
           |this is a kernel bug which will lead to unexpected behavior when using transferTo.
           |You can set spark.file.transferTo = false to disable this NIO feature.
         """.stripMargin)
    }
  }

  /**
   * A file name may contain some invalid URI characters, such as " ". This method will convert the
   * file name to a raw path accepted by `java.net.URI(String)`.
   *
   * Note: the file name must not contain "/" or "\"
   */
  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    encodeRelativeUnixPathToURIRawPath(fileName)
  }

  /**
   * Same as [[encodeFileNameToURIRawPath]] but returns the relative UNIX path.
   */
  def encodeRelativeUnixPathToURIRawPath(path: String): String = {
    require(!path.startsWith("/") && !path.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    new URI("file", null, "localhost", -1, "/" + path, null, null).getRawPath.substring(1)
  }

  /**
   * Get the file name from uri's raw path and decode it. If the raw path of uri ends with "/",
   * return the name before the last "/".
   */
  def decodeFileNameInURI(uri: URI): String = {
    val rawPath = uri.getRawPath
    val rawFileName = rawPath.split("/").last
    new URI("file:///" + rawFileName).getPath.substring(1)
  }

  /**
   * Download a file or directory to target directory. Supports fetching the file in a variety of
   * ways, including HTTP, Hadoop-compatible filesystems, and files on a standard filesystem, based
   * on the URL parameter. Fetching directories is only supported from Hadoop-compatible
   * filesystems.
   *
   * If `useCache` is true, first attempts to fetch the file to a local cache that's shared
   * across executors running the same application. `useCache` is used mainly for
   * the executors, and not in local mode.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
   *
   * If `shouldUntar` is true, it untars the given url if it is a tar.gz or tgz into `targetDir`.
   * This is a legacy behavior, and users should better use `spark.archives` configuration or
   * `SparkContext.addArchive`
   */
  def fetchFile(
      url: String,
      targetDir: File,
      conf: SparkConf,
      hadoopConf: Configuration,
      timestamp: Long,
      useCache: Boolean,
      shouldUntar: Boolean = true): File = {
    val fileName = decodeFileNameInURI(new URI(url))
    val targetFile = new File(targetDir, fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache", defaultValue = true)
    if (useCache && fetchCacheEnabled) {
      val cachedFileName = s"${url.hashCode}${timestamp}_cache"
      val lockFileName = s"${url.hashCode}${timestamp}_lock"
      // Set the cachedLocalDir for the first time and re-use it later
      if (cachedLocalDir.isEmpty) {
        this.synchronized {
          if (cachedLocalDir.isEmpty) {
            cachedLocalDir = getLocalDir(conf)
          }
        }
      }
      val localDir = new File(cachedLocalDir)
      val lockFile = new File(localDir, lockFileName)
      val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel()
      // Only one executor entry.
      // The FileLock is only used to control synchronization for executors download file,
      // it's always safe regardless of lock type (mandatory or advisory).
      val lock = lockFileChannel.lock()
      val cachedFile = new File(localDir, cachedFileName)
      try {
        if (!cachedFile.exists()) {
          doFetchFile(url, localDir, cachedFileName, conf, hadoopConf)
        }
      } finally {
        lock.release()
        lockFileChannel.close()
      }
      copyFile(
        url,
        cachedFile,
        targetFile,
        conf.getBoolean("spark.files.overwrite", false)
      )
    } else {
      doFetchFile(url, targetDir, fileName, conf, hadoopConf)
    }

    if (shouldUntar) {
      // Decompress the file if it's a .tar or .tar.gz
      if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
        logWarning(
          "Untarring behavior will be deprecated at spark.files and " +
            "SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive " +
            "instead.")
        logInfo("Untarring " + fileName)
        executeAndGetOutput(Seq("tar", "-xzf", fileName), targetDir)
      } else if (fileName.endsWith(".tar")) {
        logWarning(
          "Untarring behavior will be deprecated at spark.files and " +
            "SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive " +
            "instead.")
        logInfo("Untarring " + fileName)
        executeAndGetOutput(Seq("tar", "-xf", fileName), targetDir)
      }
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")

    // Windows does not grant read permission by default to non-admin users
    // Add read permission to owner explicitly
    if (isWindows) {
      FileUtil.chmod(targetFile.getAbsolutePath, "u+r")
    }

    targetFile
  }

  /**
   * Unpacks an archive file into the specified directory. It expects .jar, .zip, .tar.gz, .tgz
   * and .tar files. This behaves same as Hadoop's archive in distributed cache. This method is
   * basically copied from `org.apache.hadoop.yarn.util.FSDownload.unpack`.
   */
  def unpack(source: File, dest: File): Unit = {
    if (!source.exists()) {
      throw new FileNotFoundException(source.getAbsolutePath)
    }
    val lowerSrc = StringUtils.toLowerCase(source.getName)
    if (lowerSrc.endsWith(".jar")) {
      RunJar.unJar(source, dest, RunJar.MATCH_ANY)
    } else if (lowerSrc.endsWith(".zip")) {
      // After issue HADOOP-18145, unzip could keep file permissions.
      FileUtil.unZip(source, dest)
    } else if (lowerSrc.endsWith(".tar.gz") || lowerSrc.endsWith(".tgz")) {
      FileUtil.unTar(source, dest)
    } else if (lowerSrc.endsWith(".tar")) {
      // TODO(SPARK-38632): should keep file permissions. Java implementation doesn't.
      unTarUsingJava(source, dest)
    } else {
      logWarning(s"Cannot unpack $source, just copying it to $dest.")
      copyRecursive(source, dest)
    }
  }

  /**
   * The method below was copied from `FileUtil.unTar` but uses Java-based implementation
   * to work around a security issue, see also SPARK-38631.
   */
  private def unTarUsingJava(source: File, dest: File): Unit = {
    if (!dest.mkdirs && !dest.isDirectory) {
      throw new IOException(s"Mkdirs failed to create $dest")
    } else {
      try {
        // Should not fail because all Hadoop 2.1+ (from HADOOP-9264)
        // have 'unTarUsingJava'.
        val mth = classOf[FileUtil].getDeclaredMethod(
          "unTarUsingJava", classOf[File], classOf[File], classOf[Boolean])
        mth.setAccessible(true)
        mth.invoke(null, source, dest, java.lang.Boolean.FALSE)
      } catch {
        // Re-throw the original exception.
        case e: java.lang.reflect.InvocationTargetException if e.getCause != null =>
          throw e.getCause
      }
    }
  }

  /** Records the duration of running `body`. */
  def timeTakenMs[T](body: => T): (T, Long) = {
    val startTime = System.nanoTime()
    val result = body
    val endTime = System.nanoTime()
    (result, math.max(NANOSECONDS.toMillis(endTime - startTime), 0))
  }

  /**
   * Download `in` to `tempFile`, then move it to `destFile`.
   *
   * If `destFile` already exists:
   *   - no-op if its contents equal those of `sourceFile`,
   *   - throw an exception if `fileOverwrite` is false,
   *   - attempt to overwrite it otherwise.
   *
   * @param url URL that `sourceFile` originated from, for logging purposes.
   * @param in InputStream to download.
   * @param destFile File path to move `tempFile` to.
   * @param fileOverwrite Whether to delete/overwrite an existing `destFile` that does not match
   *                      `sourceFile`
   */
  private def downloadFile(
      url: String,
      in: InputStream,
      destFile: File,
      fileOverwrite: Boolean): Unit = {
    val tempFile = File.createTempFile("fetchFileTemp", null,
      new File(destFile.getParentFile.getAbsolutePath))
    logInfo(s"Fetching $url to $tempFile")

    try {
      val out = new FileOutputStream(tempFile)
      Utils.copyStream(in, out, closeStreams = true)
      copyFile(url, tempFile, destFile, fileOverwrite, removeSourceFile = true)
    } finally {
      // Catch-all for the couple of cases where for some reason we didn't move `tempFile` to
      // `destFile`.
      if (tempFile.exists()) {
        tempFile.delete()
      }
    }
  }

  /**
   * Copy `sourceFile` to `destFile`.
   *
   * If `destFile` already exists:
   *   - no-op if its contents equal those of `sourceFile`,
   *   - throw an exception if `fileOverwrite` is false,
   *   - attempt to overwrite it otherwise.
   *
   * @param url URL that `sourceFile` originated from, for logging purposes.
   * @param sourceFile File path to copy/move from.
   * @param destFile File path to copy/move to.
   * @param fileOverwrite Whether to delete/overwrite an existing `destFile` that does not match
   *                      `sourceFile`
   * @param removeSourceFile Whether to remove `sourceFile` after / as part of moving/copying it to
   *                         `destFile`.
   */
  private def copyFile(
      url: String,
      sourceFile: File,
      destFile: File,
      fileOverwrite: Boolean,
      removeSourceFile: Boolean = false): Unit = {

    if (destFile.exists) {
      if (!filesEqualRecursive(sourceFile, destFile)) {
        if (fileOverwrite) {
          logInfo(
            s"File $destFile exists and does not match contents of $url, replacing it with $url"
          )
          if (!destFile.delete()) {
            throw new SparkException(
              "Failed to delete %s while attempting to overwrite it with %s".format(
                destFile.getAbsolutePath,
                sourceFile.getAbsolutePath
              )
            )
          }
        } else {
          throw new SparkException(
            s"File $destFile exists and does not match contents of $url")
        }
      } else {
        // Do nothing if the file contents are the same, i.e. this file has been copied
        // previously.
        logInfo(
          "%s has been previously copied to %s".format(
            sourceFile.getAbsolutePath,
            destFile.getAbsolutePath
          )
        )
        return
      }
    }

    // The file does not exist in the target directory. Copy or move it there.
    if (removeSourceFile) {
      Files.move(sourceFile.toPath, destFile.toPath)
    } else {
      logInfo(s"Copying ${sourceFile.getAbsolutePath} to ${destFile.getAbsolutePath}")
      copyRecursive(sourceFile, destFile)
    }
  }

  private def filesEqualRecursive(file1: File, file2: File): Boolean = {
    if (file1.isDirectory && file2.isDirectory) {
      val subfiles1 = file1.listFiles()
      val subfiles2 = file2.listFiles()
      if (subfiles1.size != subfiles2.size) {
        return false
      }
      subfiles1.sortBy(_.getName).zip(subfiles2.sortBy(_.getName)).forall {
        case (f1, f2) => filesEqualRecursive(f1, f2)
      }
    } else if (file1.isFile && file2.isFile) {
      GFiles.equal(file1, file2)
    } else {
      false
    }
  }

  private def copyRecursive(source: File, dest: File): Unit = {
    if (source.isDirectory) {
      if (!dest.mkdir()) {
        throw new IOException(s"Failed to create directory ${dest.getPath}")
      }
      val subfiles = source.listFiles()
      subfiles.foreach(f => copyRecursive(f, new File(dest, f.getName)))
    } else {
      Files.copy(source.toPath, dest.toPath)
    }
  }

  /**
   * Download a file or directory to target directory. Supports fetching the file in a variety of
   * ways, including HTTP, Hadoop-compatible filesystems, and files on a standard filesystem, based
   * on the URL parameter. Fetching directories is only supported from Hadoop-compatible
   * filesystems.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
   */
  def doFetchFile(
      url: String,
      targetDir: File,
      filename: String,
      conf: SparkConf,
      hadoopConf: Configuration): File = {
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", defaultValue = false)
    Option(uri.getScheme).getOrElse("file") match {
      case "spark" =>
        if (SparkEnv.get == null) {
          throw new IllegalStateException(
            "Cannot retrieve files with 'spark' scheme without an active SparkEnv.")
        }
        val source = SparkEnv.get.rpcEnv.openChannel(url)
        val is = Channels.newInputStream(source)
        downloadFile(url, is, targetFile, fileOverwrite)
      case "http" | "https" | "ftp" =>
        val uc = new URL(url).openConnection()
        val timeoutMs =
          conf.getTimeAsSeconds("spark.files.fetchTimeout", "60s").toInt * 1000
        uc.setConnectTimeout(timeoutMs)
        uc.setReadTimeout(timeoutMs)
        uc.connect()
        val in = uc.getInputStream()
        downloadFile(url, in, targetFile, fileOverwrite)
      case "file" =>
        // In the case of a local file, copy the local file to the target directory.
        // Note the difference between uri vs url.
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(uri.getPath)
        copyFile(url, sourceFile, targetFile, fileOverwrite)
      case _ =>
        val fs = getHadoopFileSystem(uri, hadoopConf)
        val path = new Path(uri)
        fetchHcfsFile(path, targetDir, fs, conf, hadoopConf, fileOverwrite,
                      filename = Some(filename))
    }

    targetFile
  }

  /**
   * Fetch a file or directory from a Hadoop-compatible filesystem.
   *
   * Visible for testing
   */
  private[spark] def fetchHcfsFile(
      path: Path,
      targetDir: File,
      fs: FileSystem,
      conf: SparkConf,
      hadoopConf: Configuration,
      fileOverwrite: Boolean,
      filename: Option[String] = None): Unit = {
    if (!targetDir.exists() && !targetDir.mkdir()) {
      throw new IOException(s"Failed to create directory ${targetDir.getPath}")
    }
    val dest = new File(targetDir, filename.getOrElse(path.getName))
    if (fs.isFile(path)) {
      val in = fs.open(path)
      try {
        downloadFile(path.toString, in, dest, fileOverwrite)
      } finally {
        in.close()
      }
    } else {
      fs.listStatus(path).foreach { fileStatus =>
        fetchHcfsFile(fileStatus.getPath(), dest, fs, conf, hadoopConf, fileOverwrite)
      }
    }
  }

  /**
   * Validate that a given URI is actually a valid URL as well.
   * @param uri The URI to validate
   */
  @throws[MalformedURLException]("when the URI is an invalid URL")
  def validateURL(uri: URI): Unit = {
    Option(uri.getScheme).getOrElse("file") match {
      case "http" | "https" | "ftp" =>
        try {
          uri.toURL
        } catch {
          case e: MalformedURLException =>
            val ex = new MalformedURLException(s"URI (${uri.toString}) is not a valid URL.")
            ex.initCause(e)
            throw ex
        }
      case _ => // will not be turned into a URL anyway
    }
  }

  /**
   * Get the path of a temporary directory.  Spark's local directories can be configured through
   * multiple settings, which are used with the following precedence:
   *
   *   - If called from inside of a YARN container, this will return a directory chosen by YARN.
   *   - If the SPARK_LOCAL_DIRS environment variable is set, this will return a directory from it.
   *   - Otherwise, if the spark.local.dir is set, this will return a directory from it.
   *   - Otherwise, this will return java.io.tmpdir.
   *
   * Some of these configuration options might be lists of multiple paths, but this method will
   * always return a single directory. The return directory is chosen randomly from the array
   * of directories it gets from getOrCreateLocalRootDirs.
   */
  def getLocalDir(conf: SparkConf): String = {
    val localRootDirs = getOrCreateLocalRootDirs(conf)
    if (localRootDirs.isEmpty) {
      val configuredLocalDirs = getConfiguredLocalDirs(conf)
      throw new IOException(
        s"Failed to get a temp directory under [${configuredLocalDirs.mkString(",")}].")
    } else {
      localRootDirs(scala.util.Random.nextInt(localRootDirs.length))
    }
  }

  private[spark] def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.
    conf.getenv("CONTAINER_ID") != null
  }

  /**
   * Returns if the current codes are running in a Spark task, e.g., in executors.
   */
  def isInRunningSparkTask: Boolean = TaskContext.get() != null

  /**
   * Gets or creates the directories listed in spark.local.dir or SPARK_LOCAL_DIRS,
   * and returns only the directories that exist / could be created.
   *
   * If no directories could be created, this will return an empty list.
   *
   * This method will cache the local directories for the application when it's first invoked.
   * So calling it multiple times with a different configuration will always return the same
   * set of directories.
   */
  private[spark] def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    if (localRootDirs == null) {
      this.synchronized {
        if (localRootDirs == null) {
          localRootDirs = getOrCreateLocalRootDirsImpl(conf)
        }
      }
    }
    localRootDirs
  }

  /**
   * Return the configured local directories where Spark can write files. This
   * method does not create any directories on its own, it only encapsulates the
   * logic of locating the local directories according to deployment mode.
   */
  def getConfiguredLocalDirs(conf: SparkConf): Array[String] = {
    val shuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)
    if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available. Note this assumes that Yarn has
      // created the directories already, and that they are secured so that only the
      // user has access to them.
      randomizeInPlace(getYarnLocalDirs(conf).split(","))
    } else if (conf.getenv("SPARK_EXECUTOR_DIRS") != null) {
      conf.getenv("SPARK_EXECUTOR_DIRS").split(File.pathSeparator)
    } else if (conf.getenv("SPARK_LOCAL_DIRS") != null) {
      conf.getenv("SPARK_LOCAL_DIRS").split(",")
    } else if (conf.getenv("MESOS_SANDBOX") != null && !shuffleServiceEnabled) {
      // Mesos already creates a directory per Mesos task. Spark should use that directory
      // instead so all temporary files are automatically cleaned up when the Mesos task ends.
      // Note that we don't want this if the shuffle service is enabled because we want to
      // continue to serve shuffle files after the executors that wrote them have already exited.
      Array(conf.getenv("MESOS_SANDBOX"))
    } else {
      if (conf.getenv("MESOS_SANDBOX") != null && shuffleServiceEnabled) {
        logInfo("MESOS_SANDBOX available but not using provided Mesos sandbox because " +
          s"${config.SHUFFLE_SERVICE_ENABLED.key} is enabled.")
      }
      // In non-Yarn mode (or for the driver in yarn-client mode), we cannot trust the user
      // configuration to point to a secure directory. So create a subdirectory with restricted
      // permissions under each listed directory.
      conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    }
  }

  private def getOrCreateLocalRootDirsImpl(conf: SparkConf): Array[String] = {
    val configuredLocalDirs = getConfiguredLocalDirs(conf)
    val uris = configuredLocalDirs.filter {
      root =>
      // Here, we guess if the given value is a URI at its best - check if scheme is set.
      Try(new URI(root).getScheme != null).getOrElse(false)
    }
    if (uris.nonEmpty) {
      logWarning(
        "The configured local directories are not expected to be URIs; however, got suspicious " +
        s"values [${uris.mkString(",")}]. Please check your configured local directories.")
    }

    configuredLocalDirs.flatMap {
      root =>
      try {
        val rootDir = new File(root)
        if (rootDir.exists || rootDir.mkdirs()) {
          val dir = createTempDir(root)
          chmod700(dir)
          Some(dir.getAbsolutePath)
        } else {
          logError(s"Failed to create dir in $root. Ignoring this directory.")
          None
        }
      } catch {
        case e: IOException =>
          logError(s"Failed to create local root dir in $root. Ignoring this directory.")
          None
      }
    }
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(conf: SparkConf): String = {
    val localDirs = Option(conf.getenv("LOCAL_DIRS")).getOrElse("")

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  /** Used by unit tests. Do not call from other places. */
  private[spark] def clearLocalRootDirs(): Unit = {
    localRootDirs = null
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i + 1)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   * Note, this is typically not used from within core spark.
   */
  private lazy val localIpAddress: InetAddress = findLocalInetAddress()

  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to"
              + " a loopback address: " + address.getHostAddress + "; using "
              + strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to"
          + " a loopback address: " + address.getHostAddress + ", but we couldn't find any"
          + " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  private var customHostname: Option[String] = sys.env.get("SPARK_LOCAL_HOSTNAME")

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
   */
  def setCustomHostname(hostname: String): Unit = {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's FQDN.
   */
  def localCanonicalHostName(): String = {
    addBracketsIfNeeded(customHostname.getOrElse(localIpAddress.getCanonicalHostName))
  }

  /**
   * Get the local machine's hostname.
   * In case of IPv6, getHostAddress may return '0:0:0:0:0:0:0:1'.
   */
  def localHostName(): String = {
    addBracketsIfNeeded(customHostname.getOrElse(localIpAddress.getHostAddress))
  }

  /**
   * Get the local machine's URI.
   */
  def localHostNameForURI(): String = {
    addBracketsIfNeeded(customHostname.getOrElse(InetAddresses.toUriString(localIpAddress)))
  }

  private[spark] def addBracketsIfNeeded(addr: String): String = {
    if (addr.contains(":") && !addr.contains("[")) {
      "[" + addr + "]"
    } else {
      addr
    }
  }

  /**
   * Normalize IPv6 IPs and no-op on all other hosts.
   */
  private[spark] def normalizeIpIfNeeded(host: String): String = {
    // Is this a v6 address. We ask users to add [] around v6 addresses as strs but
    // there not always there. If it's just 0-9 and : and [] we treat it as a v6 address.
    // This means some invalid addresses are treated as v6 addresses, but since they are
    // not valid hostnames it doesn't matter.
    // See https://www.rfc-editor.org/rfc/rfc1123#page-13 for context around valid hostnames.
    val addressRe = "^[\^\[{0,1}([0-9:]+?:[0-9]*)]{0,1}$".r
    host match {
      case addressRe(unbracketed) =>
        addBracketsIfNeeded(InetAddresses.toAddrString(InetAddresses.forString(unbracketed)))
      case _ =>
        host
    }
  }

  /**
   * Checks if the host contains only valid hostname/ip without port
   * NOTE: Incase of IPV6 ip it should be enclosed inside []
   */
  def checkHost(host: String): Unit = {
    if (host != null && host.split(":").length > 2) {
      assert(host.startsWith("[") && host.endsWith("]"),
        s"Expected hostname or IPv6 IP enclosed in [] but got $host")
    } else {
      assert(host != null && host.indexOf(':') == -1, s"Expected hostname or IP but got $host")
    }
  }

  def checkHostPort(hostPort: String): Unit = {
    if (hostPort != null && hostPort.split(":").length > 2) {
      assert(hostPort != null && hostPort.indexOf("]:") != -1,
        s"Expected host and port but got $hostPort")
    } else {
      assert(hostPort != null && hostPort.indexOf(':') != -1,
        s"Expected host and port but got $hostPort")
    }
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String, Int) = {
    // Check cache first.
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    def setDefaultPortValue: (String, Int) = {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      retval
    }
    // checks if the hostport contains IPV6 ip and parses the host, port
    if (hostPort != null && hostPort.split(":").length > 2) {
      val index: Int = hostPort.lastIndexOf("]:")
      if (-1 == index) {
        return setDefaultPortValue
      }
      val port = hostPort.substring(index + 2).trim()
      val retval = (hostPort.substring(0, index + 1).trim(), if (port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort, retval)
    } else {
      val index: Int = hostPort.lastIndexOf(':')
      if (-1 == index) {
        return setDefaultPortValue
      }
      val port = hostPort.substring(index + 1).trim()
      val retval = (hostPort.substring(0, index).trim(), if (port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort, retval)
    }

    hostPortParseResults.get(hostPort)
  }

  /**
   * Return the string to tell how long has passed in milliseconds.
   * @param startTimeNs - a timestamp in nanoseconds returned by `System.nanoTime`.
   */
  def getUsedTimeNs(startTimeNs: Long): String = {
    s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms"
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  override def deleteRecursively(file: File): Unit = {
    super.deleteRecursively(file)
    if (file != null) {
      ShutdownHookManager.removeShutdownDeleteDir(file)
    }
  }

  /**
   * Determines if a directory contains any files newer than cutoff seconds.
   *
   * @param dir must be the path to a directory, or IllegalArgumentException is thrown
   * @param cutoff measured in seconds. Returns true if there are any files or directories in the
   *               given directory whose last modified time is later than this many seconds ago
   */
  def doesDirectoryContainAnyNewFiles(dir: File, cutoff: Long): Boolean = {
    if (!dir.isDirectory) {
      throw new IllegalArgumentException(s"$dir is not a directory!")
    }
    val filesAndDirs = dir.listFiles()
    val cutoffTimeInMillis = System.currentTimeMillis - (cutoff * 1000)

    filesAndDirs.exists(_.lastModified() > cutoffTimeInMillis) ||
    filesAndDirs.filter(_.isDirectory).exists(
      subdir => doesDirectoryContainAnyNewFiles(subdir, cutoff)
    )
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m, 500g) to gibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  def byteStringAsGb(str: String): Long = {
    JavaUtils.byteStringAsGb(str)
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
   */
  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MiB, because when no units are specified the unit
    // is assumed to be bytes
    (JavaUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }

  private[this] val siByteSizes =
    Array(1L << 60, 1L << 50, 1L << 40, 1L << 30, 1L << 20, 1L << 10, 1)
  private[this] val siByteSuffixes =
    Array("EiB", "PiB", "TiB", "GiB", "MiB", "KiB", "B")
  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = {
    var i = 0
    while (i < siByteSizes.length - 1 && size < 2 * siByteSizes(i)) i += 1
    "%.1f %s".formatLocal(Locale.US, size.toDouble / siByteSizes(i), siByteSuffixes(i))
  }

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    if (size.isValidLong) {
      // Common case, most sizes fit in 64 bits and all ops on BigInt are order(s) of magnitude
      // slower than Long/Double.
      bytesToString(size.toLong)
    } else if (size < BigInt(2L << 10) * EiB) {
      "%.1f EiB".formatLocal(Locale.US, BigDecimal(size) / EiB)
    } else {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    }
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute
    val locale = Locale.US

    ms match {
      case t if t < second =>
        "%d ms".formatLocal(locale, t)
      case t if t < minute =>
        "%.1f s".formatLocal(locale, t.toFloat / second)
      case t if t < hour =>
        "%.1f m".formatLocal(locale, t.toFloat / minute)
      case t =>
        "%.2f h".formatLocal(locale, t.toFloat / hour)
    }
  }

  /**
   * Convert a quantity in megabytes to a human-readable string such as "4.0 MiB".
   */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Execute a command and return the process running the command.
   */
  def executeCommand(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): Process = {
    val builder = new ProcessBuilder(command: _*).directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    if (redirectStderr) {
      val threadName = "redirect stderr for command " + command(0)
      def log(s: String): Unit = logInfo(s)
      processStreamByLine(threadName, process.getErrorStream, log)
    }
    process
  }

  /**
   * Execute a command and get its output, throwing an exception if it yields a code other than 0.
   */
  def executeAndGetOutput(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): String = {
    val process = executeCommand(command, workingDir, extraEnvironment, redirectStderr)
    val output = new StringBuilder
    val threadName = "read stdout for " + command(0)
    def appendToOutput(s: String): Unit = output.append(s).append("\n")
    val stdoutThread = processStreamByLine(threadName, process.getInputStream, appendToOutput)
    val exitCode = process.waitFor()
    stdoutThread.join()   // Wait for it to finish reading output
    if (exitCode != 0) {
      logError(s"Process $command exited with code $exitCode: $output")
      throw new SparkException(s"Process $command exited with code $exitCode")
    }
    output.toString
  }

  /**
   * Return and start a daemon thread that processes the content of the input stream line by line.
   */
  def processStreamByLine(
      threadName: String,
      inputStream: InputStream,
      processLine: String => Unit): Thread = {
    val t = new Thread(threadName) {
      override def run(): Unit = {
        for (line <- Source.fromInputStream(inputStream).getLines()) {
          processLine(line)
        }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  /**
   * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
   * default UncaughtExceptionHandler
   *
   * NOTE: This method is to be called by the spark-started JVM process.
   */
  def tryOrExit(block: => Unit): Unit = {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => sparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /**
   * Execute a block of code that evaluates to Unit, stop SparkContext if there is any uncaught
   * exception
   *
   * NOTE: This method is to be called by the driver-side components to avoid stopping the
   * user-started JVM process completely; in contrast, tryOrExit is to be called in the
   * spark-started JVM process .
   */
  def tryOrStopSparkContext(sc: SparkContext)(block: => Unit): Unit = {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if (sc != null) {
          logError(s"uncaught error in thread $currentThreadName, stopping SparkContext", t)
          sc.stopInNewThread()
        }
        if (!NonFatal(t)) {
          logError(s"throw uncaught fatal error in thread $currentThreadName", t)
          throw t
        }
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit): Unit = {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  /**
   * Execute a block of code and call the failure callbacks in the catch block. If exceptions occur
   * in either the catch or the finally block, they are appended to the list of suppressed
   * exceptions in original exception which is then rethrown.
   *
   * This is primarily an issue with `catch { abort() }` or `finally { out.close() }` blocks,
   * where the abort/close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `abort` or `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinallyAndFailureCallbacks[T](block: => T)
      (catchBlock: => Unit = (), finallyBlock: => Unit = ()): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case cause: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = cause
        try {
          logError("Aborting task", originalThrowable)
          if (TaskContext.get() != null) {
            TaskContext.get().markTaskFailed(originalThrowable)
          }
          catchBlock
        } catch {
          case t: Throwable =>
            if (originalThrowable != t) {
              originalThrowable.addSuppressed(t)
              logWarning(s"Suppressing exception in catch: ${t.getMessage}", t)
            }
        }
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }

  // A regular expression to match classes of the internal Spark API's
  // that we want to skip when finding the call site of a method.
  private val SPARK_CORE_CLASS_REGEX =
    """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]"""
  private val SPARK_SQL_CLASS_REGEX = "^org\.apache\.spark\.sql.*"