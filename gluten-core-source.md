# Apache Gluten Core: Complete Source Code

Complete source code from `/Users/warren/github/incubator-gluten/gluten-core`

---

# Table of Contents

- [adaptive](#adaptive)
- [backend](#backend)
- [caller](#caller)
- [columnar](#columnar)
- [component](#component)
- [config](#config)
- [cost](#cost)
- [enumerated](#enumerated)
- [exception](#exception)
- [execution](#execution)
- [extension](#extension)
- [gluten](#gluten)
- [hash](#hash)
- [heuristic](#heuristic)
- [initializer](#initializer)
- [injector](#injector)
- [internal](#internal)
- [iterator](#iterator)
- [jni](#jni)
- [logging](#logging)
- [memory](#memory)
- [memtarget](#memtarget)
- [metadata](#metadata)
- [offload](#offload)
- [plan](#plan)
- [planner](#planner)
- [property](#property)
- [rewrite](#rewrite)
- [shuffle](#shuffle)
- [spark](#spark)
- [storage](#storage)
- [task](#task)
- [transition](#transition)
- [util](#util)
- [utils](#utils)
- [validator](#validator)

---

# adaptive

*This section contains 2 source files.*

## GlutenCost.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/execution/adaptive/GlutenCost.scala`

```scala
package org.apache.spark.sql.execution.adaptive

/** Since https://github.com/apache/incubator-gluten/pull/6143. */
class GlutenCost(val eval: CostEvaluator, val plan: SparkPlan) extends Cost {
  override def compare(that: Cost): Int = that match {
    case that: GlutenCost if plan eq that.plan =>
      0
    case that: GlutenCost if plan == that.plan =>
      // Plans are identical. Considers the newer one as having lower cost.
      -(plan.id - that.plan.id)
    case that: GlutenCost =>
      // Plans are different. Use the delegated cost evaluator.
      assert(eval == that.eval)
      eval.evaluateCost(plan).compare(eval.evaluateCost(that.plan))
    case _ =>
      throw QueryExecutionErrors.cannotCompareCostWithTargetCostError(that.toString)
  }

  override def hashCode(): Int = throw new UnsupportedOperationException()

  override def equals(obj: Any): Boolean = obj match {
    case that: Cost => compare(that) == 0
    case _ => false
  }
}
```

---

## GlutenCostEvaluator.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/execution/adaptive/GlutenCostEvaluator.scala`

```scala
package org.apache.spark.sql.execution.adaptive

/**
 * This [[CostEvaluator]] is to force use the new physical plan when cost is equal.
 *
 * Since https://github.com/apache/incubator-gluten/pull/6143.
 */
case class GlutenCostEvaluator() extends CostEvaluator with SQLConfHelper {

  private val vanillaCostEvaluator: CostEvaluator = {
    if (SparkVersionUtil.lteSpark32) {
      val clazz = Utils.classForName("org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator$")
      clazz.getDeclaredField("MODULE$").get(null).asInstanceOf[CostEvaluator]
    } else {
      val forceOptimizeSkewedJoin =
        conf.getConfString("spark.sql.adaptive.forceOptimizeSkewedJoin").toBoolean
      val clazz = Utils.classForName("org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator")
      val ctor = clazz.getConstructor(classOf[Boolean])
      ctor.newInstance(forceOptimizeSkewedJoin.asInstanceOf[Object]).asInstanceOf[CostEvaluator]
    }
  }

  override def evaluateCost(plan: SparkPlan): Cost = {
    if (GlutenCoreConfig.get.enableGluten) {
      new GlutenCost(vanillaCostEvaluator, plan)
    } else {
      vanillaCostEvaluator.evaluateCost(plan)
    }
  }
}
```

---

# backend

*This section contains 1 source files.*

## Backend.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/backend/Backend.scala`

```scala
package org.apache.gluten.backend

trait Backend extends Component {

  /**
   * Backends don't have dependencies. They are all considered root components in the component DAG
   * and will be loaded at the beginning.
   */
  final override def dependencies(): Seq[Class[_ <: Component]] = Nil
}
```

---

# caller

*This section contains 1 source files.*

## CallerInfo.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/caller/CallerInfo.scala`

```scala
package org.apache.gluten.extension.caller

/**
 * Helper API that stores information about the call site of the columnar rule. Specific columnar
 * rules could call the API to check whether this time of rule call was initiated for certain
 * purpose. For example, a rule call could be for AQE optimization, or for cached plan optimization,
 * or for regular executed plan optimization.
 */
trait CallerInfo {
  def isAqe(): Boolean
  def isCache(): Boolean
  def isStreaming(): Boolean
}

object CallerInfo {
  private val localStorage: ThreadLocal[Option[CallerInfo]] =
    new ThreadLocal[Option[CallerInfo]]() {
      override def initialValue(): Option[CallerInfo] = None
    }

  private class Impl(
      override val isAqe: Boolean,
      override val isCache: Boolean,
      override val isStreaming: Boolean
  ) extends CallerInfo

  /*
   * Find the information about the caller that initiated the rule call.
   */
  def create(): CallerInfo = {
    if (localStorage.get().nonEmpty) {
      return localStorage.get().get
    }
    val stack = Thread.currentThread.getStackTrace
    new Impl(
      isAqe = inAqeCall(stack),
      isCache = inCacheCall(stack),
      isStreaming = inStreamingCall(stack))
  }

  private def inAqeCall(stack: Seq[StackTraceElement]): Boolean = {
    stack.exists(_.getClassName.equals(AdaptiveSparkPlanExec.getClass.getName))
  }

  private def inCacheCall(stack: Seq[StackTraceElement]): Boolean = {
    stack.exists(_.getClassName.equals(InMemoryRelation.getClass.getName))
  }

  private def inStreamingCall(stack: Seq[StackTraceElement]): Boolean = {
    stack.exists(_.getClassName.equals(StreamExecution.getClass.getName.split('$').head))
  }

  /** For testing only. */
  def withLocalValue[T](isAqe: Boolean, isCache: Boolean, isStreaming: Boolean = false)(
      body: => T): T = {
    val prevValue = localStorage.get()
    val newValue = new Impl(isAqe, isCache, isStreaming)
    localStorage.set(Some(newValue))
    try {
      body
    } finally {
      localStorage.set(prevValue)
    }
  }
}
```

---

# columnar

*This section contains 4 source files.*

## ColumnarRuleApplier.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/ColumnarRuleApplier.scala`

```scala
package org.apache.gluten.extension.columnar

trait ColumnarRuleApplier {
  def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan
}

object ColumnarRuleApplier {
  class ColumnarRuleCall(
      val session: SparkSession,
      val caller: CallerInfo,
      val outputsColumnar: Boolean) {
    val sqlConf = session.sessionState.conf
  }
}
```

---

## ColumnarRuleExecutor.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/ColumnarRuleExecutor.scala`

```scala
package org.apache.gluten.extension.columnar

class ColumnarRuleExecutor(phase: String, rules: Seq[Rule[SparkPlan]])
  extends RuleExecutor[SparkPlan] {
  private val batch: Batch = Batch(s"Columnar (Phase [$phase])", Once, rules: _*)

  // TODO: Remove this exclusion then manage to pass Spark's idempotence check.
  override protected val excludedOnceBatches: Set[String] = Set(batch.name)

  override protected def batches: Seq[Batch] = Seq(batch)
}
```

---

## FallbackTag.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/FallbackTag.scala`

```scala
package org.apache.gluten.extension.columnar

sealed trait FallbackTag {
  val stacktrace: Option[String] =
    if (FallbackTags.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None

  def reason(): String
}

object FallbackTag {

  /** A tag that stores one reason text of fall back. */
  case class Appendable(override val reason: String) extends FallbackTag

  /**
   * A tag that stores reason text of fall back. Other reasons will be discarded when this tag is
   * added to plan.
   */
  case class Exclusive(override val reason: String) extends FallbackTag

  trait Converter[T] {
    def from(obj: T): Option[FallbackTag]
  }

  object Converter {
    implicit def asIs[T <: FallbackTag]: Converter[T] = (tag: T) => Some(tag)

    implicit object FromString extends Converter[String] {
      override def from(reason: String): Option[FallbackTag] = Some(Appendable(reason))
    }
  }
}

object FallbackTags {
  val TAG: TreeNodeTag[FallbackTag] =
    TreeNodeTag[FallbackTag]("org.apache.gluten.FallbackTag")

  val DEBUG = false

  /**
   * If true, the plan node will be guaranteed fallback to Vanilla plan node while being
   * implemented.
   *
   * If false, the plan still has chance to be turned into "non-transformable" in any another
   * validation rule. So user should not consider the plan "transformable" unless all validation
   * rules are passed.
   */
  def nonEmpty(plan: SparkPlan): Boolean = {
    getOption(plan).nonEmpty
  }

  /**
   * If true, it implies the plan maybe transformable during validation phase but not guaranteed,
   * since another validation rule could turn it to "non-transformable" before implementing the plan
   * within Gluten transformers. If false, the plan node will be guaranteed fallback to Vanilla plan
   * node while being implemented.
   */
  def maybeOffloadable(plan: SparkPlan): Boolean = !nonEmpty(plan)

  def add[T](plan: TreeNode[_], t: T)(implicit converter: FallbackTag.Converter[T]): Unit = {
    val tagOption = getOption(plan)
    val newTagOption = converter.from(t)

    val mergedTagOption: Option[FallbackTag] =
      (tagOption ++ newTagOption).reduceOption[FallbackTag] {
        // New tag comes while the plan was already tagged, merge.
        case (_, exclusive: FallbackTag.Exclusive) =>
          exclusive
        case (exclusive: FallbackTag.Exclusive, _) =>
          exclusive
        case (l: FallbackTag.Appendable, r: FallbackTag.Appendable) =>
          FallbackTag.Appendable(s"${l.reason}; ${r.reason}")
      }
    mergedTagOption
      .foreach(mergedTag => plan.setTagValue(TAG, mergedTag))
  }

  def untag(plan: TreeNode[_]): Unit = {
    plan.unsetTagValue(TAG)
  }

  def get(plan: TreeNode[_]): FallbackTag = {
    getOption(plan).getOrElse(
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.toString()))
  }

  def getOption(plan: TreeNode[_]): Option[FallbackTag] = {
    plan.getTagValue(TAG)
  }
}

case class RemoveFallbackTagRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(FallbackTags.untag)
    plan
  }
}
```

---

## SparkCacheUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/execution/columnar/SparkCacheUtil.scala`

```scala
package org.apache.spark.sql.execution.columnar

object SparkCacheUtil {
  def clearCacheSerializer(): Unit = {
    InMemoryRelation.clearSerializer()
  }
}
```

---

# component

*This section contains 5 source files.*

## Component.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/component/Component.scala`

```scala
package org.apache.gluten.component

/**
 * The base API to inject user-defined logic to Gluten. To register a component, the implementation
 * class of this trait should be placed to Gluten's classpath with a component file. Gluten will
 * discover all the component implementations then register them at the booting time.
 *
 * See [[Discovery]] to find more information about how the component files are handled.
 */
@Experimental
trait Component {
  private val uid = nextUid.getAndIncrement()
  private val isRegistered = new AtomicBoolean(false)

  def ensureRegistered(): Unit = {
    if (!isRegistered.compareAndSet(false, true)) {
      return
    }
    graph.add(this)
    dependencies().foreach(req => graph.declareDependency(this, req))
  }

  /** Base information. */
  def name(): String
  def buildInfo(): BuildInfo
  def dependencies(): Seq[Class[_ <: Component]]

  /** Spark listeners. */
  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}
  def onDriverShutdown(): Unit = {}
  def onExecutorStart(pc: PluginContext): Unit = {}
  def onExecutorShutdown(): Unit = {}

  /** Metrics register, only called on Driver. */
  def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {}

  /**
   * Overrides [[org.apache.gluten.extension.columnar.transition.ConventionFunc]] Gluten is using to
   * determine the convention (its row-based processing / columnar-batch processing support) of a
   * plan with a user-defined function that accepts a plan then returns convention type it outputs,
   * and input conventions it requires.
   */
  def convFuncOverride(): ConventionFunc.Override = ConventionFunc.Override.Empty

  /**
   * A sequence of [[org.apache.gluten.extension.columnar.cost.LongCoster]] Gluten is using for cost
   * evaluation.
   */
  def costers(): Seq[LongCoster] = Nil

  /** Query planner rules. */
  def injectRules(injector: Injector): Unit
}

object Component {
  private val nextUid = new AtomicInteger()
  private val graph: Graph = new Graph()

  // format: off
  /**
   * Apply topology sort on all registered components in graph to get an ordered list of
   * components. The root nodes will be on the head side of the list, while leaf nodes
   * will be on the tail side of the list.
   *
   * Say if component-A depends on component-B while component-C requires nothing, then the
   * output order will be one of the following:
   *
   *   1. [component-B, component-A, component-C]
   *   2. [component-C, component-B, component-A]
   *   3. [component-B, component-C, component-A]
   *
   * By all means component B will be placed before component A because of the declared
   * dependency from component A to component B.
   *
   * @throws UnsupportedOperationException When cycles in dependency graph are found.
   */
  // format: on
  def sorted(): Seq[Component] = {
    ensureAllComponentsRegistered()
    graph.sorted()
  }

  private[component] def sortedUnsafe(): Seq[Component] = {
    graph.sorted()
  }

  private class Registry {
    private val lookupByUid: mutable.Map[Int, Component] = mutable.Map()
    private val lookupByClass: mutable.Map[Class[_ <: Component], Component] = mutable.Map()

    def register(comp: Component): Unit = synchronized {
      val uid = comp.uid
      val clazz = comp.getClass
      require(!lookupByUid.contains(uid), s"Component UID $uid already registered: ${comp.name()}")
      require(
        !lookupByClass.contains(clazz),
        s"Component class $clazz already registered: ${comp.name()}")
      lookupByUid += uid -> comp
      lookupByClass += clazz -> comp
    }

    def isUidRegistered(uid: Int): Boolean = synchronized {
      lookupByUid.contains(uid)
    }

    def isClassRegistered(clazz: Class[_ <: Component]): Boolean = synchronized {
      lookupByClass.contains(clazz)
    }

    def findByClass(clazz: Class[_ <: Component]): Component = synchronized {
      require(lookupByClass.contains(clazz))
      lookupByClass(clazz)
    }

    def findByUid(uid: Int): Component = synchronized {
      require(lookupByUid.contains(uid))
      lookupByUid(uid)
    }

    def allUids(): Seq[Int] = synchronized {
      return lookupByUid.keys.toSeq
    }
  }

  private class Graph {
    private val registry: Registry = new Registry()
    private val dependencies: mutable.Buffer[(Int, Class[_ <: Component])] = mutable.Buffer()

    private var sortedComponents: Option[Seq[Component]] = None

    def add(comp: Component): Unit = synchronized {
      require(
        !registry.isUidRegistered(comp.uid),
        s"Component UID ${comp.uid} already registered: ${comp.name()}")
      require(
        !registry.isClassRegistered(comp.getClass),
        s"Component class ${comp.getClass} already registered: ${comp.name()}")
      registry.register(comp)
      sortedComponents = None
    }

    def declareDependency(comp: Component, dependencyCompClass: Class[_ <: Component]): Unit =
      synchronized {
        require(registry.isUidRegistered(comp.uid))
        require(registry.isClassRegistered(comp.getClass))
        dependencies += comp.uid -> dependencyCompClass
        sortedComponents = None
      }

    private def newLookup(): mutable.Map[Int, Node] = {
      val lookup: mutable.Map[Int, Node] = mutable.Map()

      registry.allUids().foreach {
        uid =>
          require(!lookup.contains(uid))
          val n = new Node(uid)
          lookup += uid -> n
      }

      dependencies.foreach {
        case (uid, dependencyCompClass) =>
          require(
            registry.isClassRegistered(dependencyCompClass),
            s"Dependency class not registered yet: ${dependencyCompClass.getName}")
          val dependencyUid = registry.findByClass(dependencyCompClass).uid
          require(uid != dependencyUid)
          require(lookup.contains(uid))
          require(lookup.contains(dependencyUid))
          val n = lookup(uid)
          val r = lookup(dependencyUid)
          require(!n.parents.contains(r.uid))
          require(!r.children.contains(n.uid))
          n.parents += r.uid -> r
          r.children += n.uid -> n
      }

      lookup
    }

    def sorted(): Seq[Component] = synchronized {
      if (sortedComponents.isDefined) {
        return sortedComponents.get
      }

      val lookup: mutable.Map[Int, Node] = newLookup()

      val out = mutable.Buffer[Component]()
      val uidToNumParents = lookup.map { case (uid, node) => uid -> node.parents.size }
      val removalQueue = mutable.Queue[Int]()

      // 1. Find out all nodes with zero parents then enqueue them.
      uidToNumParents.filter(_._2 == 0).foreach(kv => removalQueue.enqueue(kv._1))

      // 2. Loop to dequeue and remove nodes from the uid-to-num-parents map.
      while (removalQueue.nonEmpty) {
        val parentUid = removalQueue.dequeue()
        val node = lookup(parentUid)
        out += registry.findByUid(parentUid)
        node.children.keys.foreach {
          childUid =>
            uidToNumParents += childUid -> (uidToNumParents(childUid) - 1)
            val updatedNumParents = uidToNumParents(childUid)
            assert(updatedNumParents >= 0)
            if (updatedNumParents == 0) {
              removalQueue.enqueue(childUid)
            }
        }
      }

      // 3. If there are still outstanding nodes (those are with more non-zero parents) in the
      // uid-to-num-parents map, then it means at least one cycle is found. Report error if so.
      if (uidToNumParents.exists(_._2 != 0)) {
        val cycleNodes = uidToNumParents.filter(_._2 != 0).keys.map(registry.findByUid)
        val cycleNodeNames = cycleNodes.map(_.name()).mkString(", ")
        throw new UnsupportedOperationException(
          s"Cycle detected in the component graph: $cycleNodeNames")
      }

      // 4. Return the ordered nodes.
      sortedComponents = Some(out.toSeq)
      sortedComponents.get
    }
  }

  private object Graph {
    class Node(val uid: Int) {
      val parents: mutable.Map[Int, Node] = mutable.Map()
      val children: mutable.Map[Int, Node] = mutable.Map()
    }
  }

  case class BuildInfo(name: String, branch: String, revision: String, revisionTime: String)
}
```

---

## Discovery.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/component/Discovery.scala`

```scala
package org.apache.gluten.component

// format: off

/**
 * Gluten's global discovery to find all [[Component]] definitions in the classpath.
 *
 * We don't use [[java.util.ServiceLoader]] since it requires all the service files to have
 * the same file name which is the class name of [[Component]], this causes the service files
 * easily be overwritten by each other during Maven build. Typically, See code of
 * `DefaultMavenFileFilter` used by Maven's `maven-resources-plugin`.
 *
 * Instead, Gluten defines its own way to register components. For example, placing the following
 * component files to resource folder:
 *
 *  META-INF
 *  \- gluten-components
 *     |- org.apache.gluten.component.AComponent
 *     \- org.apache.gluten.backend.BBackend
 *
 * Will cause the registration of component `AComponent` and backend `BBackend`.
 *
 * The content in a component file is not read so doesn't matter at the moment.
 */
// format: on
private object Discovery extends Logging {
  private val container: String = "META-INF/gluten-components"
  private val componentFilePattern: Regex = s"^(.+)$$".r

  def discoverAll(): Seq[Component] = {
    logInfo("Start discovering components in the current classpath... ")
    val prev = System.currentTimeMillis()
    val allFiles = ResourceUtil.getResources(container, componentFilePattern.pattern).asScala
    val duration = System.currentTimeMillis() - prev
    logInfo(s"Discovered component files: ${allFiles.mkString(", ")}. Duration: $duration ms.")
    val deDup = mutable.Set[String]()
    val out = allFiles.flatMap {
      case componentFilePattern(className) =>
        if (!deDup.add(className)) {
          logWarning(s"Found duplicated component class $className in then classpath, ignoring.")
          None
        } else {
          val clazz =
            try {
              SparkReflectionUtil.classForName(className)
            } catch {
              case e: ClassNotFoundException =>
                throw new GlutenException(s"Component class not found: $className", e)
            }
          val instance = clazz.getDeclaredConstructor().newInstance().asInstanceOf[Component]
          Some(instance)
        }
      case _ => None
    }.toSeq
    out
  }
}
```

---

## package.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/component/package.scala`

```scala
package org.apache.gluten

package object component extends Logging {
  private val allComponentsLoaded: AtomicBoolean = new AtomicBoolean(false)

  private[component] def ensureAllComponentsRegistered(): Unit = {
    if (!allComponentsLoaded.compareAndSet(false, true)) {
      return
    }

    // Load all components in classpath.
    val all = Discovery.discoverAll()

    // Register all components.
    all.foreach(_.ensureRegistered())

    // Output log so user could view the component loading order.
    // Call #sortedUnsafe than on #sorted to avoid unnecessary recursion.
    val components = Component.sortedUnsafe()
    require(
      components.nonEmpty,
      s"No component files found in container directories named with " +
        s"'META-INF/gluten-components' from classpath. JVM classpath value " +
        s"is: ${System.getProperty("java.class.path")}"
    )
    logInfo(s"Components registered within order: ${components.map(_.name()).mkString(", ")}")
  }
}
```

---

## ComponentSuite.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/gluten/component/ComponentSuite.scala`

```scala
package org.apache.gluten.component

class ComponentSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val d = new DummyComponentD()
  d.ensureRegistered()
  private val b = new DummyBackendB()
  b.ensureRegistered()
  private val a = new DummyBackendA()
  a.ensureRegistered()
  private val c = new DummyComponentC()
  c.ensureRegistered()
  private val e = new DummyComponentE()
  e.ensureRegistered()

  test("Load order - sanity") {
    val possibleOrders =
      Set(
        Seq(a, b, c, d, e),
        Seq(a, b, d, c, e),
        Seq(b, a, c, d, e),
        Seq(b, a, d, c, e)
      )

    assert(possibleOrders.contains(Component.sorted()))
  }

  test("Register again") {
    assertThrows[IllegalArgumentException] {
      new DummyBackendA().ensureRegistered()
    }
  }
}

object ComponentSuite {
  private class DummyBackendA extends Backend {
    override def name(): String = "dummy-backend-a"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_BACKEND_A", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyBackendB extends Backend {
    override def name(): String = "dummy-backend-b"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_BACKEND_B", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyComponentC extends Component {
    override def dependencies(): Seq[Class[_ <: Component]] = classOf[DummyBackendA] :: Nil

    override def name(): String = "dummy-component-c"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_COMPONENT_C", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyComponentD extends Component {
    override def dependencies(): Seq[Class[_ <: Component]] =
      Seq(classOf[DummyBackendA], classOf[DummyBackendB])

    override def name(): String = "dummy-component-d"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_COMPONENT_D", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }

  private class DummyComponentE extends Component {
    override def dependencies(): Seq[Class[_ <: Component]] =
      Seq(classOf[DummyBackendA], classOf[DummyComponentD])

    override def name(): String = "dummy-component-e"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_COMPONENT_E", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
  }
}
```

---

## WithDummyBackend.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/gluten/component/WithDummyBackend.scala`

```scala
package org.apache.gluten.component

trait WithDummyBackend extends BeforeAndAfterAll {
  this: Suite =>
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    DummyBackend.ensureRegistered()
  }
}

object WithDummyBackend {
  object DummyBackend extends Backend {
    override def name(): String = "dummy-backend"
    override def buildInfo(): Component.BuildInfo =
      Component.BuildInfo("DUMMY_BACKEND", "N/A", "N/A", "N/A")
    override def injectRules(injector: Injector): Unit = {}
    override def costers(): Seq[LongCoster] = Seq(new LongCoster {
      override def kind(): LongCostModel.Kind = Legacy
      override def selfCostOf(node: SparkPlan): Option[Long] = Some(1)
    })
  }
}
```

---

# config

*This section contains 4 source files.*

## ConfigBuilder.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/config/ConfigBuilder.scala`

```scala
package org.apache.gluten.config

object BackendType extends Enumeration {
  type BackendType = Value
  val COMMON, VELOX, CLICKHOUSE = Value
}

private[gluten] case class ConfigBuilder(key: String) {
  private[config] var _doc = ""
  private[config] var _version = ""
  private[config] var _backend = BackendType.COMMON
  private[config] var _public = true
  private[config] var _experimental = false
  private[config] var _alternatives = List.empty[String]
  private[config] var _onCreate: Option[ConfigEntry[_] => Unit] = None

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(s: String): ConfigBuilder = {
    _version = s
    this
  }

  def backend(backend: BackendType.BackendType): ConfigBuilder = {
    _backend = backend
    this
  }

  /**
   * This method marks a config as internal for any of the following reasons:
   *   - Intended exclusively for developers or advanced users
   *   - Allows for flexibility in development and testing without compromising the public API's
   *     stability
   */
  def internal(): ConfigBuilder = {
    _public = false
    this
  }

  def experimental(): ConfigBuilder = {
    _experimental = true
    this
  }

  def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
    _onCreate = Option(callback)
    this
  }

  def withAlternative(key: String): ConfigBuilder = {
    _alternatives = _alternatives :+ key
    this
  }

  def intConf: TypedConfigBuilder[Int] = {
    new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
  }

  def longConf: TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    new TypedConfigBuilder(this, toBoolean(_, key))
  }

  def stringConf: TypedConfigBuilder[String] = {
    new TypedConfigBuilder(this, identity)
  }

  def timeConf(unit: TimeUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, timeFromString(_, unit), timeToString(_, unit))
  }

  def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
    new TypedConfigBuilder(this, byteFromString(_, unit), byteToString(_, unit))
  }

  def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T] = {
    val entry =
      new ConfigEntryFallback[T](
        key,
        _doc,
        _version,
        _backend,
        _public,
        _experimental,
        _alternatives,
        fallback)
    _onCreate.foreach(_(entry))
    entry
  }
}

private object ConfigHelpers {
  def toNumber[T](s: String, converter: String => T, key: String, configType: String): T = {
    try {
      converter(s.trim)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"$key should be $configType, but was $s")
    }
  }

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  private val TIME_STRING_PATTERN = Pattern.compile("(-?[0-9]+)([a-z]+)?")

  def timeFromString(str: String, unit: TimeUnit): Long = JavaUtils.timeStringAs(str, unit)

  def timeToString(v: Long, unit: TimeUnit): String = s"${TimeUnit.MILLISECONDS.convert(v, unit)}ms"

  def byteFromString(str: String, unit: ByteUnit): Long = {
    val (input, multiplier) =
      if (str.length() > 0 && str.charAt(0) == '-') {
        (str.substring(1), -1)
      } else {
        (str, 1)
      }
    multiplier * JavaUtils.byteStringAs(input, unit)
  }

  def byteToString(v: Long, unit: ByteUnit): String = s"${unit.convertTo(v, ByteUnit.BYTE)}b"
}

private[gluten] class TypedConfigBuilder[T](
    val parent: ConfigBuilder,
    val converter: String => T,
    val stringConverter: T => String) {

  def this(parent: ConfigBuilder, converter: String => T) = {
    this(parent, converter, { v: T => v.toString })
  }

  def transform(fn: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
    transform {
      v =>
        if (!validator(v)) {
          throw new IllegalArgumentException(s"'$v' in ${parent.key} is invalid. $errorMsg")
        }
        v
    }
  }

  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform {
      v =>
        if (!validValues.contains(v)) {
          throw new IllegalArgumentException(
            s"The value of ${parent.key} should be one of ${validValues.mkString(", ")}, " +
              s"but was $v")
        }
        v
    }
  }

  def createOptional: OptionalConfigEntry[T] = {
    val entry = new OptionalConfigEntry[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter)
    parent._onCreate.foreach(_(entry))
    entry
  }

  def createWithDefault(default: T): ConfigEntry[T] = {
    assert(default != null, "Use createOptional.")
    default match {
      case str: String => createWithDefaultString(str)
      case _ =>
        val transformedDefault = converter(stringConverter(default))
        val entry = new ConfigEntryWithDefault[T](
          parent.key,
          parent._doc,
          parent._version,
          parent._backend,
          parent._public,
          parent._experimental,
          parent._alternatives,
          converter,
          stringConverter,
          transformedDefault
        )
        parent._onCreate.foreach(_(entry))
        entry
    }
  }

  def createWithDefaultString(default: String): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultString[T](
      parent.key,
      parent._doc,
      parent._version,
      parent._backend,
      parent._public,
      parent._experimental,
      parent._alternatives,
      converter,
      stringConverter,
      default
    )
    parent._onCreate.foreach(_(entry))
    entry
  }
}
```

---

## ConfigEntry.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/config/ConfigEntry.scala`

```scala
package org.apache.gluten.config

/**
 * An entry contains all meta information for a configuration.
 *
 * The code is similar to Spark's relevant config code but extended for Gluten's use, like adding
 * backend type, etc.
 *
 * @tparam T
 *   the value type
 */
trait ConfigEntry[T] {

  /** The key for the configuration. */
  def key: String

  /** The documentation for the configuration. */
  def doc: String

  /** The gluten version when the configuration was released. */
  def version: String

  /** The backend type of the configuration. */
  def backend: BackendType.BackendType

  /**
   * If this configuration is public to the user. If it's `false`, this configuration is only used
   * internally and we should not expose it to users.
   */
  def isPublic: Boolean

  def isExperimental: Boolean

  /** the alternative keys for the configuration. */
  def alternatives: List[String]

  /**
   * How to convert a string to the value. It should throw an exception if the string does not have
   * the required format.
   */
  def valueConverter: String => T

  /** How to convert a value to a string that the user can use it as a valid string value. */
  def stringConverter: T => String

  /** Read the configuration from the given GlutenConfigProvider. */
  def readFrom(conf: GlutenConfigProvider): T

  /** The default value of the configuration. */
  def defaultValue: Option[T]

  /** The string representation of the default value. */
  def defaultValueString: String

  final protected def readString(provider: GlutenConfigProvider): Option[String] = {
    alternatives.foldLeft(provider.get(key))((res, nextKey) => res.orElse(provider.get(nextKey)))
  }

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version)"
  }
}

private[gluten] class OptionalConfigEntry[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String)
  extends ConfigEntry[Option[T]] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => Option[T] = s => Option(_valueConverter(s))

  override def stringConverter: Option[T] => String = v => v.map(_stringConverter).orNull

  override def readFrom(conf: GlutenConfigProvider): Option[T] =
    readString(conf).map(_valueConverter)

  override def defaultValue: Option[Option[T]] = None

  override def defaultValueString: String = ConfigEntry.UNDEFINED
}

private[gluten] class ConfigEntryWithDefault[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultVal: T)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(_defaultVal)
  }

  override def defaultValue: Option[T] = Option(_defaultVal)

  override def defaultValueString: String = stringConverter(_defaultVal)
}

private[gluten] class ConfigEntryWithDefaultString[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    _valueConverter: String => T,
    _stringConverter: T => String,
    _defaultVal: String)
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = _valueConverter

  override def stringConverter: T => String = _stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    val value = readString(conf).getOrElse(_defaultVal)
    valueConverter(value)
  }

  override def defaultValue: Option[T] = Some(valueConverter(_defaultVal))

  override def defaultValueString: String = _defaultVal
}

private[gluten] class ConfigEntryFallback[T](
    _key: String,
    _doc: String,
    _version: String,
    _backend: BackendType,
    _isPublic: Boolean,
    _isExperimental: Boolean,
    _alternatives: List[String],
    fallback: ConfigEntry[T])
  extends ConfigEntry[T] {
  override def key: String = _key

  override def doc: String = _doc

  override def version: String = _version

  override def backend: BackendType = _backend

  override def isPublic: Boolean = _isPublic

  override def isExperimental: Boolean = _isExperimental

  override def alternatives: List[String] = _alternatives

  override def valueConverter: String => T = fallback.valueConverter

  override def stringConverter: T => String = fallback.stringConverter

  override def readFrom(conf: GlutenConfigProvider): T = {
    readString(conf).map(valueConverter).getOrElse(fallback.readFrom(conf))
  }

  override def defaultValue: Option[T] = fallback.defaultValue

  override def defaultValueString: String = fallback.defaultValueString
}

object ConfigEntry {
  val UNDEFINED = "<undefined>"
}
```

---

## ConfigRegistry.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/config/ConfigRegistry.scala`

```scala
package org.apache.gluten.config

trait ConfigRegistry {
  private val configEntries =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]().asScala

  private def register(entry: ConfigEntry[_]): Unit = {
    val existing = configEntries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Config entry ${entry.key} already registered!")
  }

  /** Visible for testing. */
  private[config] def allEntries: Seq[ConfigEntry[_]] = {
    configEntries.values.toSeq
  }

  protected def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate {
      entry =>
        register(entry)
        ConfigRegistry.registerToAllEntries(entry)
    }
  }

  protected def buildStaticConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate {
      entry =>
        SQLConf.registerStaticConfigKey(key)
        register(entry)
        ConfigRegistry.registerToAllEntries(entry)
    }
  }

  def get: GlutenCoreConfig
}

object ConfigRegistry {
  private val allConfigEntries =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]().asScala

  private def registerToAllEntries(entry: ConfigEntry[_]): Unit = {
    val existing = allConfigEntries.putIfAbsent(entry.key, entry)
    require(existing.isEmpty, s"Config entry ${entry.key} already registered!")
  }

  def containsEntry(entry: ConfigEntry[_]): Boolean = {
    allConfigEntries.contains(entry.key)
  }

  def findEntry(key: String): Option[ConfigEntry[_]] = allConfigEntries.get(key)
}
```

---

## GlutenCoreConfig.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/config/GlutenCoreConfig.scala`

```scala
package org.apache.gluten.config

class GlutenCoreConfig(conf: SQLConf) extends Logging {
  private lazy val configProvider = new SQLConfProvider(conf)

  def getConf[T](entry: ConfigEntry[T]): T = {
    require(ConfigRegistry.containsEntry(entry), s"$entry is not registered")
    entry.readFrom(configProvider)
  }

  def enableGluten: Boolean = getConf(GLUTEN_ENABLED)

  def enableRas: Boolean = getConf(RAS_ENABLED)

  def rasCostModel: String = getConf(RAS_COST_MODEL)

  def memoryUntracked: Boolean = getConf(COLUMNAR_MEMORY_UNTRACKED)

  def offHeapMemorySize: Long = getConf(COLUMNAR_OFFHEAP_SIZE_IN_BYTES)

  def taskOffHeapMemorySize: Long = getConf(COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES)

  def conservativeTaskOffHeapMemorySize: Long =
    getConf(COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES)

  def memoryIsolation: Boolean = getConf(COLUMNAR_MEMORY_ISOLATION)

  def memoryOverAcquiredRatio: Double = getConf(COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO)

  def memoryReservationBlockSize: Long = getConf(COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE)

  def dynamicOffHeapSizingEnabled: Boolean =
    getConf(DYNAMIC_OFFHEAP_SIZING_ENABLED)

  def dynamicOffHeapSizingMemoryFraction: Double =
    getConf(DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION)
}

/*
 * Note: Gluten configuration.md is automatically generated from this code.
 * Make sure to run dev/gen_all_config_docs.sh after making changes to this file.
 */
object GlutenCoreConfig extends ConfigRegistry {
  override def get: GlutenCoreConfig = {
    new GlutenCoreConfig(SQLConf.get)
  }

  val SPARK_OFFHEAP_SIZE_KEY = "spark.memory.offHeap.size"
  val SPARK_OFFHEAP_ENABLED_KEY = "spark.memory.offHeap.enabled"

  val SPARK_ONHEAP_SIZE_KEY = "spark.executor.memory"

  val GLUTEN_ENABLED =
    buildConf("spark.gluten.enabled")
      .doc(
        "Whether to enable gluten. Default value is true. Just an experimental property." +
          " Recommend to enable/disable Gluten through the setting for spark.plugins.")
      .booleanConf
      .createWithDefault(true)

  // Options used by RAS.
  val RAS_ENABLED =
    buildConf("spark.gluten.ras.enabled")
      .doc(
        "Enables RAS (relational algebra selector) during physical " +
          "planning to generate more efficient query plan. Note, this feature doesn't bring " +
          "performance profits by default. Try exploring option `spark.gluten.ras.costModel` " +
          "for advanced usage.")
      .booleanConf
      .createWithDefault(false)

  // FIXME: This option is no longer only used by RAS. Should change key to
  //  `spark.gluten.costModel` or something similar.
  val RAS_COST_MODEL =
    buildConf("spark.gluten.ras.costModel")
      .doc(
        "The class name of user-defined cost model that will be used by Gluten's transition " +
          "planner as well as by RAS. If not specified, a legacy built-in cost model will be " +
          "used. The legacy cost model helps RAS planner exhaustively offload computations, and " +
          "helps transition planner choose columnar-to-columnar transition over others.")
      .stringConf
      .createWithDefaultString("legacy")

  val COLUMNAR_MEMORY_UNTRACKED =
    buildStaticConf("spark.gluten.memory.untracked")
      .internal()
      .doc(
        "When enabled, turn all native memory allocations in Gluten into untracked. Spark " +
          "will be unaware of the allocations so will not trigger spill-to-disk operations " +
          "or Spark OOMs. Should only be used for testing or other non-production use cases.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_MEMORY_ISOLATION =
    buildConf("spark.gluten.memory.isolation")
      .doc("Enable isolated memory mode. If true, Gluten controls the maximum off-heap memory " +
        "can be used by each task to X, X = executor memory / max task slots. It's recommended " +
        "to set true if Gluten serves concurrent queries within a single session, since not all " +
        "memory Gluten allocated is guaranteed to be spillable. In the case, the feature should " +
        "be enabled to avoid OOM.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_OVERHEAD_SIZE_IN_BYTES =
    buildConf("spark.gluten.memoryOverhead.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.task.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES =
    buildConf("spark.gluten.memory.conservative.task.offHeap.size.in.bytes")
      .internal()
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("0")

  val COLUMNAR_MEMORY_OVER_ACQUIRED_RATIO =
    buildConf("spark.gluten.memory.overAcquiredMemoryRatio")
      .doc("If larger than 0, Velox backend will try over-acquire this ratio of the total " +
        "allocated memory as backup to avoid OOM.")
      .doubleConf
      .checkValue(d => d >= 0.0d, "Over-acquired ratio should be larger than or equals 0")
      .createWithDefault(0.3d)

  val COLUMNAR_MEMORY_RESERVATION_BLOCK_SIZE =
    buildConf("spark.gluten.memory.reservationBlockSize")
      .doc("Block size of native reservation listener reserve memory from Spark.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8MB")

  val NUM_TASK_SLOTS_PER_EXECUTOR =
    buildConf("spark.gluten.numTaskSlotsPerExecutor")
      .doc(
        "Must provide default value since non-execution operations " +
          "(e.g. org.apache.spark.sql.Dataset#summary) doesn't propagate configurations using " +
          "org.apache.spark.sql.execution.SQLExecution#withSQLConfPropagated")
      .intConf
      .createWithDefaultString("-1")

  // Since https://github.com/apache/incubator-gluten/issues/5439.
  val DYNAMIC_OFFHEAP_SIZING_ENABLED =
    buildStaticConf("spark.gluten.memory.dynamic.offHeap.sizing.enabled")
      .experimental()
      .doc(
        "Experimental: When set to true, the offheap config (spark.memory.offHeap.size) will " +
          "be ignored and instead we will consider onheap and offheap memory in combination, " +
          "both counting towards the executor memory config (spark.executor.memory). We will " +
          "make use of JVM APIs to determine how much onheap memory is use, alongside tracking " +
          "offheap allocations made by Gluten. We will then proceed to enforcing a total memory " +
          "quota, calculated by the sum of what memory is committed and in use in the Java " +
          "heap. Since the calculation of the total quota happens as offheap allocation happens " +
          "and not as JVM heap memory is allocated, it is possible that we can oversubscribe " +
          "memory. Additionally, note that this change is experimental and may have performance " +
          "implications.")
      .booleanConf
      .createWithDefault(false)

  // Since https://github.com/apache/incubator-gluten/issues/5439.
  val DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION =
    buildStaticConf("spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction")
      .experimental()
      .doc(
        "Experimental: Determines the memory fraction used to determine the total " +
          "memory available for offheap and onheap allocations when the dynamic offheap " +
          "sizing feature is enabled. The default is set to match spark.executor.memoryFraction.")
      .doubleConf
      .checkValue(v => v >= 0 && v <= 1, "offheap sizing memory fraction must between [0, 1]")
      .createWithDefault(0.6)
}
```

---

# cost

*This section contains 6 source files.*

## GlutenCost.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/cost/GlutenCost.scala`

```scala
package org.apache.gluten.extension.columnar.cost

trait GlutenCost
```

---

## GlutenCostModel.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/cost/GlutenCostModel.scala`

```scala
package org.apache.gluten.extension.columnar.cost

// format: off
/**
 * The cost model API of Gluten. Used by:
 * <p>
 *   1. RAS planner for cost-based optimization;
 * <p>
 *   2. Transition graph for choosing transition paths.
 */
// format: on
trait GlutenCostModel {
  def costOf(node: SparkPlan): GlutenCost
  def costComparator(): Ordering[GlutenCost]
  def makeZeroCost(): GlutenCost
  def makeInfCost(): GlutenCost
  // Returns cost value of one + other.
  def sum(one: GlutenCost, other: GlutenCost): GlutenCost
  // Returns cost value of one - other.
  def diff(one: GlutenCost, other: GlutenCost): GlutenCost
}

object GlutenCostModel extends Logging {
  private val costModelRegistry = {
    val r = LongCostModel.registry()
    // Components should override Backend's costers. Hence, reversed registration order is applied.
    Component
      .sorted()
      .reverse
      .flatMap(_.costers())
      .foreach(coster => r.register(coster))
    r
  }

  def find(aliasOrClass: String): GlutenCostModel = {
    val costModel = find(costModelRegistry, aliasOrClass)
    costModel
  }

  private def find(registry: LongCostModel.Registry, aliasOrClass: String): GlutenCostModel = {
    if (LongCostModel.Kind.values().contains(aliasOrClass)) {
      val kind = LongCostModel.Kind.values()(aliasOrClass)
      val model = registry.get(kind)
      return model
    }
    val clazz = SparkReflectionUtil.classForName(aliasOrClass)
    logInfo(s"Using user cost model: $aliasOrClass")
    val ctor = clazz.getDeclaredConstructor()
    ctor.setAccessible(true)
    val model: GlutenCostModel = ctor.newInstance().asInstanceOf[GlutenCostModel]
    model
  }
}
```

---

## LongCost.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/cost/LongCost.scala`

```scala
package org.apache.gluten.extension.columnar.cost

case class LongCost(value: Long) extends GlutenCost
```

---

## LongCostModel.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/cost/LongCostModel.scala`

```scala
package org.apache.gluten.extension.columnar.cost

abstract class LongCostModel extends GlutenCostModel {
  private val infLongCost = Long.MaxValue
  private val zeroLongCost = 0

  override def costOf(node: SparkPlan): LongCost = node match {
    case _: GroupLeafExec => throw new IllegalStateException()
    case _ => LongCost(longCostOf(node))
  }

  // Sum with ceil to avoid overflow.
  private def safeSum(a: Long, b: Long): Long = {
    assert(a >= 0)
    assert(b >= 0)
    val sum = a + b
    if (sum < a || sum < b) infLongCost else sum
  }

  override def sum(one: GlutenCost, other: GlutenCost): LongCost = (one, other) match {
    case (LongCost(value), LongCost(otherValue)) => LongCost(safeSum(value, otherValue))
  }

  // Returns cost value of one - other.
  override def diff(one: GlutenCost, other: GlutenCost): GlutenCost = (one, other) match {
    case (LongCost(value), LongCost(otherValue)) =>
      val d = Math.subtractExact(value, otherValue)
      require(d >= zeroLongCost, s"Difference between cost $one and $other should not be negative")
      LongCost(d)
  }

  private def longCostOf(node: SparkPlan): Long = node match {
    case n =>
      val selfCost = selfLongCostOf(n)
      (n.children.map(longCostOf).toSeq :+ selfCost).reduce[Long](safeSum)
  }

  def selfLongCostOf(node: SparkPlan): Long

  override def costComparator(): Ordering[GlutenCost] = Ordering.Long.on {
    case LongCost(value) => value
    case _ => throw new IllegalStateException("Unexpected cost type")
  }

  override def makeInfCost(): GlutenCost = LongCost(infLongCost)
  override def makeZeroCost(): GlutenCost = LongCost(zeroLongCost)
}

object LongCostModel extends Logging {
  def registry(): Registry = {
    new Registry()
  }

  /**
   * Kind of a cost model. Output of #name() will be used as alias to identify the cost model
   * instance from the registry.
   */
  sealed trait Kind {
    all.synchronized {
      val n = name()
      if (all.contains(n)) {
        throw new GlutenException(s"Cost mode kind $n already registered")
      }
      all += n -> this
    }

    def name(): String
  }

  object Kind {
    private val all: mutable.Map[String, Kind] = mutable.Map()
    def values(): Map[String, Kind] = all.toMap
  }

  /**
   * A cost model that is supposed to drive RAS planner create the same query plan with legacy
   * planner.
   */
  case object Legacy extends Kind {
    override def name(): String = "legacy"
  }

  /** A rough cost model with some empirical heuristics. */
  case object Rough extends Kind {
    override def name(): String = "rough"
  }

  class Registry private[LongCostModel] {
    private val lookup: mutable.Map[Kind, LongCosterChain.Builder] = mutable.Map()

    // The registered coster will take lower precedence than all the existing
    // registered costers in cost estimation.
    def register(coster: LongCoster): Unit = synchronized {
      val chainBuilder = builderOf(coster.kind())
      chainBuilder.register(coster)
    }

    def get(kind: Kind): LongCostModel = synchronized {
      builderOf(kind).build()
    }

    private def builderOf(kind: Kind): LongCosterChain.Builder = {
      lookup.getOrElseUpdate(kind, LongCosterChain.builder())
    }
  }
}
```

---

## LongCoster.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/cost/LongCoster.scala`

```scala
package org.apache.gluten.extension.columnar.cost

/**
 * Costs one single Spark plan node. The coster returns none if the input plan node is not
 * recognizable.
 *
 * Used by the composite cost model [[LongCosterChain]].
 */
trait LongCoster {

  /** The coster will be registered as part of the cost model associated with this kind. */
  def kind(): LongCostModel.Kind

  /**
   * Calculates the long integer cost of the input query plan node. Note, this calculation should
   * omit children's costs.
   */
  def selfCostOf(node: SparkPlan): Option[Long]
}
```

---

## LongCosterChain.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/cost/LongCosterChain.scala`

```scala
package org.apache.gluten.extension.columnar.cost

/**
 * A [[LongCostModel]] implementation that consists of a set of sub-costers.
 *
 * The costers will apply in the same order they were registered or added.
 */
private class LongCosterChain private (costers: Seq[LongCoster]) extends LongCostModel {
  override def selfLongCostOf(node: SparkPlan): Long = {
    // Applies the costers respectively, returns when a coster gives a meaningful non-none number.
    // If all costers give none, throw an error.
    costers
      .foldLeft[Option[Long]](None) {
        case (None, coster) =>
          coster.selfCostOf(node)
        case (c @ Some(_), _) =>
          c
      }
      .getOrElse(throw new GlutenException(s"Cost not found for node: $node"))
  }
}

object LongCosterChain {
  def builder(): Builder = new Builder()

  class Builder private[LongCosterChain] {
    private val costers = mutable.ListBuffer[LongCoster]()
    private var out: Option[LongCosterChain] = None

    def register(coster: LongCoster): Builder = synchronized {
      costers += coster
      out = None
      this
    }

    private[cost] def build(): LongCosterChain = synchronized {
      if (out.isEmpty) {
        out = Some(new LongCosterChain(costers.toSeq))
      }
      return out.get
    }
  }
}
```

---

# enumerated

*This section contains 2 source files.*

## EnumeratedApplier.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/EnumeratedApplier.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by enumerating on
 * all the possibilities of executable Gluten plans, then choose the best plan among them.
 *
 * NOTE: We still have a bunch of heuristic rules in this implementation's rule list. Future work
 * will include removing them from the list then implementing them in EnumeratedTransform.
 */
class EnumeratedApplier(
    session: SparkSession,
    ruleBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    ruleWrappers: Seq[Rule[SparkPlan] => Rule[SparkPlan]])
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {

  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val call = new ColumnarRuleCall(session, CallerInfo.create(), outputsColumnar)
    val finalPlan = apply0(
      ruleBuilders
        .map(b => b(call))
        .map(r => ruleWrappers.foldLeft(r) { case (r, wrapper) => wrapper(r) }),
      plan)
    finalPlan
  }

  private def apply0(rules: Seq[Rule[SparkPlan]], plan: SparkPlan): SparkPlan =
    new ColumnarRuleExecutor("ras", rules).execute(plan)
}

object EnumeratedApplier {}
```

---

## EnumeratedTransform.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/EnumeratedTransform.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated

/**
 * Rule to offload Spark query plan to Gluten query plan using a search algorithm and a defined cost
 * model.
 *
 * The effect of this rule is similar to
 * [[org.apache.gluten.extension.columnar.heuristic.HeuristicTransform]], except that the 3 stages
 * in the heuristic version, known as rewrite, validate, offload, will take place together
 * individually for each Spark query plan node in RAS rule
 * [[org.apache.gluten.extension.columnar.enumerated.RasOffload]].
 *
 * The feature requires enabling RAS to function.
 */
case class EnumeratedTransform(costModel: GlutenCostModel, rules: Seq[RasRule[SparkPlan]])
  extends Rule[SparkPlan]
  with LogLevelUtil {
  private val optimization = {
    GlutenOptimization
      .builder()
      .costModel(asRasCostModel(costModel))
      .addRules(rules)
      .create()
  }

  private val convReq = Conv.any

  override def apply(plan: SparkPlan): SparkPlan = {
    val constraintSet = PropertySet(Seq(convReq))
    val planner = optimization.newPlanner(plan, constraintSet)
    val out = planner.plan()
    out
  }
}

object EnumeratedTransform {
  // Creates a static EnumeratedTransform rule for use in certain
  // places that requires to emulate the offloading of a Spark query plan.
  //
  // TODO: Avoid using this and eventually remove the API.
  def static(): EnumeratedTransform = {
    val exts = new SparkSessionExtensions()
    val dummyInjector = new Injector(exts)
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    Component.sorted().reverse.foreach(_.injectRules(dummyInjector))
    val session = SparkSession.getActiveSession.getOrElse(
      throw new GlutenException(
        "HeuristicTransform#static can only be called when an active Spark session exists"))
    val call = new ColumnarRuleCall(session, CallerInfo.create(), false)
    dummyInjector.gluten.ras.createEnumeratedTransform(call)
  }

  def asRasCostModel(gcm: GlutenCostModel): CostModel[SparkPlan] = {
    new CostModelAdapter(gcm)
  }

  /** The adapter to make GlutenCostModel comply with RAS cost model. */
  private class CostModelAdapter(gcm: GlutenCostModel) extends CostModel[SparkPlan] {
    override def costOf(node: SparkPlan): Cost = CostAdapter(gcm.costOf(node))
    override def costComparator(): Ordering[Cost] =
      gcm.costComparator().on[Cost] { case CostAdapter(gc) => gc }
    override def makeInfCost(): Cost = CostAdapter(gcm.makeInfCost())
  }

  private case class CostAdapter(gc: GlutenCost) extends Cost
}
```

---

# exception

*This section contains 1 source files.*

## GlutenException.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/exception/GlutenException.java`

```java
package org.apache.gluten.exception;

public class GlutenException extends RuntimeException {

  public GlutenException() {}

  public GlutenException(String message) {
    super(message);
  }

  public GlutenException(String message, Throwable cause) {
    super(message, cause);
  }

  public GlutenException(Throwable cause) {
    super(cause);
  }
}
```

---

# execution

*This section contains 4 source files.*

## ColumnarToColumnarExec.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/execution/ColumnarToColumnarExec.scala`

```scala
package org.apache.gluten.execution

abstract class ColumnarToColumnarExec(from: Convention.BatchType, to: Convention.BatchType)
  extends ColumnarToColumnarTransition
  with GlutenPlan {

  override def isSameConvention: Boolean = from == to

  def child: SparkPlan

  protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch]

  protected def closeIterator(out: Iterator[ColumnarBatch]): Unit = {}

  protected def needRecyclePayload: Boolean = false

  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
      "selfTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to convert batches")
    )

  override def batchType(): Convention.BatchType = to

  override def rowType0(): Convention.RowType = {
    Convention.RowType.None
  }

  override def requiredChildConvention(): Seq[ConventionReq] = {
    List(ConventionReq.ofBatch(ConventionReq.BatchType.Is(from)))
  }

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val selfTime = longMetric("selfTime")

    child.executeColumnar().mapPartitions {
      in =>
        // Self millis = Out millis - In millis.
        val selfMillis = new AtomicLong(0L)
        val wrappedIn = Iterators
          .wrap(in)
          .collectReadMillis(inMillis => selfMillis.getAndAdd(-inMillis))
          .create()
          .map {
            inBatch =>
              numInputRows += inBatch.numRows()
              numInputBatches += 1
              inBatch
          }
        val out = mapIterator(wrappedIn)
        val builder = Iterators
          .wrap(out)
          .protectInvocationFlow()
          .collectReadMillis(outMillis => selfMillis.getAndAdd(outMillis))
          .recycleIterator {
            closeIterator(out)
            selfTime += selfMillis.get()
          }
        if (needRecyclePayload) {
          builder.recyclePayload(_.close())
        }
        builder
          .create()
          .map {
            outBatch =>
              numOutputRows += outBatch.numRows()
              numOutputBatches += 1
              outBatch
          }
    }

  }

  override def output: Seq[Attribute] = child.output
}
```

---

## ColumnarToColumnarTransition.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/execution/ColumnarToColumnarTransition.scala`

```scala
package org.apache.gluten.execution

trait ColumnarToColumnarTransition extends UnaryExecNode {
  def isSameConvention: Boolean
}
```

---

## GlutenPlan.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/execution/GlutenPlan.scala`

```scala
package org.apache.gluten.execution

/**
 * Base interface for Query plan that defined by backends.
 *
 * The following Spark APIs are marked final so forbidden from overriding:
 *   - supportsColumnar
 *   - supportsRowBased (Spark version >= 3.3)
 *
 * Instead, subclasses are expected to implement the following APIs:
 *   - batchType
 *   - rowType0
 *   - requiredChildConvention (optional)
 *
 * With implementations of the APIs provided, Gluten query planner will be able to find and insert
 * proper transitions between different plan nodes.
 *
 * Implementing `requiredChildConvention` is optional while the default implementation is a sequence
 * of convention reqs that are exactly the same with the output convention. If it's not the case for
 * some plan types, then the API should be overridden. For example, a typical row-to-columnar
 * transition is at the same time a query plan node that requires for row input however produces
 * columnar output.
 */
trait GlutenPlan
  extends SparkPlan
  with Convention.KnownBatchType
  with Convention.KnownRowTypeForSpark33OrLater
  with GlutenPlan.SupportsRowBasedCompatible
  with ConventionReq.KnownChildConvention {

  final override val supportsColumnar: Boolean = {
    batchType() != Convention.BatchType.None
  }

  final override val supportsRowBased: Boolean = {
    rowType() != Convention.RowType.None
  }

  override def batchType(): Convention.BatchType

  override def rowType0(): Convention.RowType

  override def requiredChildConvention(): Seq[ConventionReq] = {
    // In the normal case, children's convention should follow parent node's convention.
    val childReq = Convention.of(rowType(), batchType()).asReq()
    Seq.tabulate(children.size)(
      _ => {
        childReq
      })
  }
}

object GlutenPlan {
  // To be compatible with Spark (version < 3.3)
  trait SupportsRowBasedCompatible {
    def supportsRowBased(): Boolean = {
      throw new GlutenException("Illegal state: The method is not expected to be called")
    }
  }
}
```

---

## SparkPlanUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/execution/SparkPlanUtil.scala`

```scala
package org.apache.spark.sql.execution

/**
 * This is a hack to access withNewChildrenInternal method of TreeNode, which is a protected method
 */
object SparkPlanUtil {
  def withNewChildrenInternal(plan: SparkPlan, children: IndexedSeq[SparkPlan]): SparkPlan = {
    // 1. Get the Method object for the protected method
    val method: Method =
      classOf[TreeNode[_]].getDeclaredMethod("withNewChildrenInternal", classOf[IndexedSeq[_]])

    // 2. Make it accessible, bypassing the 'protected' modifier
    method.setAccessible(true)

    // 3. Invoke the method on the specific instance of the SparkPlan
    //    and cast the result to the expected type SparkPlan.
    method.invoke(plan, children).asInstanceOf[SparkPlan]
  }
}
```

---

# extension

*This section contains 2 source files.*

## GlutenColumnarRule.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/GlutenColumnarRule.scala`

```scala
package org.apache.gluten.extension

object GlutenColumnarRule {
  // Utilities to infer columnar rule's caller's property:
  // ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
  private case class DummyRowOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  private case class DummyColumnarOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }
}

case class GlutenColumnarRule(
    session: SparkSession,
    applierBuilder: SparkSession => ColumnarRuleApplier)
  extends ColumnarRule
  with Logging
  with LogLevelUtil {

  /**
   * Note: Do not implement this API. We basically inject all of Gluten's physical rules through
   * `postColumnarTransitions`.
   *
   * See: https://github.com/oap-project/gluten/pull/4790
   */
  final override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    // To infer caller's property: ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
    if (plan.supportsColumnar) {
      DummyColumnarOutputExec(plan)
    } else {
      DummyRowOutputExec(plan)
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    val (originalPlan, outputsColumnar) = plan match {
      case DummyRowOutputExec(child) =>
        (child, false)
      case RowToColumnarExec(DummyRowOutputExec(child)) =>
        (child, true)
      case DummyColumnarOutputExec(child) =>
        (child, true)
      case ColumnarToRowExec(DummyColumnarOutputExec(child)) =>
        (child, false)
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave an issue at" +
            " https://github.com/apache/incubator-gluten.")
    }
    val vanillaPlan = Transitions.insert(originalPlan, outputsColumnar)
    val applier = applierBuilder.apply(session)
    val out = applier.apply(vanillaPlan, outputsColumnar)
    out
  }
}
```

---

## GlutenSessionExtensions.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/GlutenSessionExtensions.scala`

```scala
package org.apache.gluten.extension

private[gluten] class GlutenSessionExtensions
  extends (SparkSessionExtensions => Unit)
  with Logging {
  override def apply(exts: SparkSessionExtensions): Unit = {
    val injector = new Injector(exts)
    injector.control.disableOn {
      session =>
        val glutenEnabledGlobally = session.conf
          .get(
            GlutenCoreConfig.GLUTEN_ENABLED.key,
            GlutenCoreConfig.GLUTEN_ENABLED.defaultValueString)
          .toBoolean
        val disabled = !glutenEnabledGlobally
        logDebug(s"Gluten is disabled by variable: glutenEnabledGlobally: $glutenEnabledGlobally")
        disabled
    }
    injector.control.disableOn {
      session =>
        val glutenEnabledForThread =
          Option(session.sparkContext.getLocalProperty(GLUTEN_ENABLE_FOR_THREAD_KEY))
            .forall(_.toBoolean)
        val disabled = !glutenEnabledForThread
        logDebug(s"Gluten is disabled by variable: glutenEnabledForThread: $glutenEnabledForThread")
        disabled
    }
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    Component.sorted().reverse.foreach(_.injectRules(injector))
    injector.inject()
  }
}

object GlutenSessionExtensions {
  val GLUTEN_SESSION_EXTENSION_NAME: String = classOf[GlutenSessionExtensions].getCanonicalName
  val GLUTEN_ENABLE_FOR_THREAD_KEY: String = "gluten.enabledForCurrentThread"
}
```

---

# gluten

*This section contains 1 source files.*

## GlutenPlugin.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/GlutenPlugin.scala`

```scala
package org.apache.gluten

class GlutenPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = {
    new GlutenDriverPlugin()
  }

  override def executorPlugin(): ExecutorPlugin = {
    new GlutenExecutorPlugin()
  }
}

private[gluten] class GlutenDriverPlugin extends DriverPlugin with Logging {
  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    val conf = pluginContext.conf()
    // Spark SQL extensions
    val extensionSeq = conf.get(SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty)
    if (!extensionSeq.toSet.contains(GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME)) {
      conf.set(
        SPARK_SESSION_EXTENSIONS,
        extensionSeq :+ GlutenSessionExtensions.GLUTEN_SESSION_EXTENSION_NAME)
    }

    setPredefinedConfigs(conf)

    val components = Component.sorted()
    printComponentInfo(components)
    components.foreach(_.onDriverStart(sc, pluginContext))
    Collections.emptyMap()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    Component.sorted().foreach(_.registerMetrics(appId, pluginContext))
  }

  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onDriverShutdown())
  }
}

private object GlutenDriverPlugin extends Logging {
  private def checkOffHeapSettings(conf: SparkConf): Unit = {
    if (conf.get(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED)) {
      // When dynamic off-heap sizing is enabled, off-heap mode is not strictly required to be
      // enabled. Skip the check.
      return
    }

    if (conf.get(GlutenCoreConfig.COLUMNAR_MEMORY_UNTRACKED)) {
      // When untracked memory mode is enabled, off-heap mode is not strictly required to be
      // enabled. Skip the check.
      return
    }

    val minOffHeapSize = "1MB"
    if (
      !conf.getBoolean(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, defaultValue = false) ||
      conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, 0) < JavaUtils.byteStringAsBytes(
        minOffHeapSize)
    ) {
      throw new GlutenException(
        s"Must set '${GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY}' to true " +
          s"and set '${GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY}' to be greater " +
          s"than $minOffHeapSize")
    }
  }

  private def setPredefinedConfigs(conf: SparkConf): Unit = {
    // check memory off-heap enabled and size.
    checkOffHeapSettings(conf)

    // Get the off-heap size set by user.
    val offHeapSize =
      if (conf.getBoolean(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED.key, false)) {
        val onHeapSize: Long =
          if (conf.contains(GlutenCoreConfig.SPARK_ONHEAP_SIZE_KEY)) {
            conf.getSizeAsBytes(GlutenCoreConfig.SPARK_ONHEAP_SIZE_KEY)
          } else {
            // 1GB default
            1024 * 1024 * 1024
          }
        ((onHeapSize - (300 * 1024 * 1024)) *
          conf.getDouble(GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_MEMORY_FRACTION.key, 0.6d)).toLong
      } else {
        conf.getSizeAsBytes(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY)
      }

    // Set off-heap size in bytes.
    conf.set(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES, offHeapSize)

    // Set off-heap size in bytes per task.
    val taskSlots = SparkResourceUtil.getTaskSlots(conf)
    conf.set(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR, taskSlots)
    val offHeapPerTask = offHeapSize / taskSlots
    conf.set(GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES, offHeapPerTask)

    // Pessimistic off-heap sizes, with the assumption that all non-borrowable storage memory
    // determined by spark.memory.storageFraction was used.
    val fraction = 1.0d - conf.getDouble("spark.memory.storageFraction", 0.5d)
    val conservativeOffHeapPerTask = (offHeapSize * fraction).toLong / taskSlots
    conf.set(
      GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES,
      conservativeOffHeapPerTask)
  }

  private def printComponentInfo(components: Seq[Component]): Unit = {
    val componentInfo = mutable.LinkedHashMap[String, String]()
    componentInfo.put("Components", components.map(_.buildInfo().name).mkString(", "))
    components.foreach {
      comp =>
        val buildInfo = comp.buildInfo()
        componentInfo.put(s"Component ${buildInfo.name} Branch", buildInfo.branch)
        componentInfo.put(s"Component ${buildInfo.name} Revision", buildInfo.revision)
        componentInfo.put(s"Component ${buildInfo.name} Revision Time", buildInfo.revisionTime)
    }
    val loggingInfo = componentInfo
      .map { case (name, value) => s"$name: $value" }
      .mkString(
        "Gluten components:\n==============================================================\n",
        "\n",
        "\n=============================================================="
      )
    logInfo(loggingInfo)
  }
}

private[gluten] class GlutenExecutorPlugin extends ExecutorPlugin {
  private val taskListeners: Seq[TaskListener] = Seq(TaskResources)

  /** Initialize the executor plugin. */
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    CodedInputStreamClassInitializer.modifyDefaultRecursionLimitUnsafe
    // Initialize Backend.
    Component.sorted().foreach(_.onExecutorStart(ctx))
  }

  /** Clean up and terminate this plugin. For example: close the native engine. */
  override def shutdown(): Unit = {
    Component.sorted().reverse.foreach(_.onExecutorShutdown())
    super.shutdown()
  }

  override def onTaskStart(): Unit = {
    taskListeners.foreach(_.onTaskStart())
  }

  override def onTaskSucceeded(): Unit = {
    taskListeners.reverse.foreach(_.onTaskSucceeded())
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    taskListeners.reverse.foreach(_.onTaskFailed(failureReason))
  }
}

private object GlutenPlugin {}
```

---

# hash

*This section contains 2 source files.*

## ConsistentHash.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/hash/ConsistentHash.java`

```java
package org.apache.gluten.hash;

/**
 * A consistent hash ring implementation. the class is thread-safe.
 *
 * <p>It is mainly used to request across multiple nodes in a distributed system in a more balanced
 * and stable way, minimizing the impact when nodes are added or removed.
 *
 * <p>The ConsistentHash support virtual nodes by replication of the physical nodes, called
 * partition. It allocates the partition to the ring by hashing the partition key to a slot in the
 * ring.
 *
 * @param <T> the type of node to be used in the ring.
 */
@ThreadSafe
public class ConsistentHash<T extends ConsistentHash.Node> {

  // read-write lock for the ring.
  private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

  // keep the mapping of node to its partitions, each partition is a slot in the ring.
  private final Map<T, Set<Partition<T>>> nodes = new HashMap<>();

  // keep the mapping of slot to partition, the partition actually is a virtual node.
  private final SortedMap<Long, Partition<T>> ring = new TreeMap<>();

  // the number of virtual nodes for each physical node.
  private final int replicate;

  // the hasher function to hash the key.
  private final Hasher hasher;

  public ConsistentHash(int replicate) {
    Preconditions.checkArgument(replicate > 0, "HashRing require positive replicate number.");
    this.replicate = replicate;
    this.hasher =
        (key, seed) -> {
          byte[] data = key.getBytes();
          return MurmurHash3.hash32x86(data, 0, data.length, seed);
        };
  }

  public ConsistentHash(int replicate, Hasher hasher) {
    Preconditions.checkArgument(replicate > 0, "HashRing require positive replicate number.");
    Preconditions.checkArgument(hasher != null, "HashRing require non-null hasher.");
    this.replicate = replicate;
    this.hasher = hasher;
  }

  /**
   * Add a node to the ring, the node will be replicated to `replicate` virtual nodes.
   *
   * @param node the node to be added to the ring.
   * @return true if the node is added successfully, false otherwise.
   */
  public boolean addNode(T node) {
    lock.writeLock().lock();
    try {
      return add(node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Remove the node from the ring, all the virtual nodes will be removed.
   *
   * @param node the node to be removed.
   * @return true if the node is removed successfully, false otherwise.
   */
  public boolean removeNode(T node) {
    lock.writeLock().lock();
    boolean removed = false;
    try {
      if (nodes.containsKey(node)) {
        Set<Partition<T>> partitions = nodes.remove(node);
        partitions.forEach(p -> ring.remove(p.getSlot()));
        removed = true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    return removed;
  }

  /**
   * Allocate the node by the key, the number of nodes to be located is specified by the count.
   *
   * @param key the key to locate the node.
   * @param count the number of nodes to be located.
   * @return a set of nodes located by the key.
   */
  public Set<T> allocateNodes(String key, int count) {
    lock.readLock().lock();
    try {
      Set<T> res = new HashSet<>();
      if (key != null && count > 0) {
        if (count < nodes.size()) {
          long slot = hasher.hash(key, 0);
          Iterator<Partition<T>> it = new AllocateIterator(slot);
          while (it.hasNext() && res.size() < count) {
            Partition<T> part = it.next();
            res.add(part.getNode());
          }
        } else {
          res.addAll(nodes.keySet());
        }
      }
      return res;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get all the nodes in the ring.
   *
   * @return a set of nodes in the ring.
   */
  public Set<T> getNodes() {
    lock.readLock().lock();
    try {
      return new HashSet<>(nodes.keySet());
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the partitions of the node. return null if the node is not exists.
   *
   * @param node the node to get the partitions.
   * @return a set of partitions of the node.
   */
  public Set<Partition<T>> getPartition(T node) {
    lock.readLock().lock();
    try {
      return nodes.get(node);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check if the node is in the ring.
   *
   * @param node the node to be checked.
   * @return true if the node is in the ring, false otherwise.
   */
  public boolean contains(T node) {
    lock.readLock().lock();
    try {
      return nodes.containsKey(node);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Check if the ring contains the slot.
   *
   * @param slot the slot to be checked.
   * @return true if the slot is in the ring, false otherwise.
   */
  public boolean ringContain(long slot) {
    lock.readLock().lock();
    try {
      return ring.containsKey(slot);
    } finally {
      lock.readLock().unlock();
    }
  }

  private boolean add(T node) {
    boolean added = false;
    if (node != null && !nodes.containsKey(node)) {
      Set<Partition<T>> partitions =
          IntStream.range(0, replicate)
              .mapToObj(idx -> new Partition<T>(node, idx))
              .collect(Collectors.toSet());
      nodes.put(node, partitions);

      // allocate slot.
      for (Partition<T> partition : partitions) {
        long slot;
        int seed = 0;
        do {
          slot = this.hasher.hash(partition.getPartitionKey(), seed++);
        } while (ring.containsKey(slot));

        partition.setSlot(slot);
        ring.put(slot, partition);
      }
      added = true;
    }
    return added;
  }

  public static class Partition<T extends ConsistentHash.Node> {
    private final T node;

    private final int index;

    private long slot;

    public Partition(T node, int index) {
      this.node = node;
      this.index = index;
    }

    public String getPartitionKey() {
      return String.format("%s:%d", node, index);
    }

    public T getNode() {
      return node;
    }

    public void setSlot(long slot) {
      this.slot = slot;
    }

    public long getSlot() {
      return this.slot;
    }
  }

  /** Base interface for the node in the ring. */
  public interface Node {
    String key();
  }

  /** Base interface for the hash function. */
  public interface Hasher {
    long hash(String key, int seed);
  }

  private class AllocateIterator implements Iterator<Partition<T>> {
    private final Iterator<Partition<T>> head;
    private final Iterator<Partition<T>> tail;

    AllocateIterator(long slot) {
      this.head = ring.headMap(slot).values().iterator();
      this.tail = ring.tailMap(slot).values().iterator();
    }

    @Override
    public boolean hasNext() {
      return head.hasNext() || tail.hasNext();
    }

    @Override
    public Partition<T> next() {
      return tail.hasNext() ? tail.next() : head.next();
    }
  }
}
```

---

## ConsistentHashTest.java

**Path**: `../incubator-gluten/gluten-core/src/test/java/org/apache/gluten/hash/ConsistentHashTest.java`

```java
package org.apache.gluten.hash;

public class ConsistentHashTest {

  private ConsistentHash<ConsistentHash.Node> consistentHash;
  private static final int REPLICAS = 3;

  @Before
  public void setUp() throws Exception {
    consistentHash = new ConsistentHash<>(REPLICAS);
  }

  @Test
  public void testAddNode() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));
    Assert.assertEquals(10, consistentHash.getNodes().size());

    HostNode existsNode = new HostNode("executor-1");
    HostNode nonExistsNode = new HostNode("executor-100");
    Assert.assertTrue(consistentHash.contains(existsNode));
    Assert.assertFalse(consistentHash.contains(nonExistsNode));

    Set<ConsistentHash.Partition<ConsistentHash.Node>> existsPartitions =
        consistentHash.getPartition(existsNode);
    Assert.assertEquals(REPLICAS, existsPartitions.size());
    Set<ConsistentHash.Partition<ConsistentHash.Node>> nonExistsPartitions =
        consistentHash.getPartition(nonExistsNode);
    Assert.assertNull(nonExistsPartitions);
  }

  @Test
  public void testRemoveNode() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));
    HostNode existsNode = new HostNode("executor-11");
    consistentHash.addNode(existsNode);
    Assert.assertEquals(11, consistentHash.getNodes().size());
    Set<ConsistentHash.Partition<ConsistentHash.Node>> partitions =
        consistentHash.getPartition(existsNode);
    Assert.assertEquals(REPLICAS, partitions.size());
    ConsistentHash.Partition<ConsistentHash.Node> partition = partitions.iterator().next();
    Assert.assertTrue(consistentHash.ringContain(partition.getSlot()));
    consistentHash.removeNode(existsNode);
    Assert.assertEquals(10, consistentHash.getNodes().size());
    Set<ConsistentHash.Partition<ConsistentHash.Node>> removedPartitions =
        consistentHash.getPartition(existsNode);
    Assert.assertNull(removedPartitions);
    Assert.assertFalse(consistentHash.ringContain(partition.getSlot()));
  }

  @Test
  public void testContain() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));
    HostNode existsNode = new HostNode("executor-11");
    consistentHash.addNode(existsNode);
    Assert.assertTrue(consistentHash.contains(existsNode));
  }

  @Test
  public void testAllocateNodes() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));

    Set<ConsistentHash.Node> allocateNodes =
        consistentHash.allocateNodes("part-00000-38af6778-964a-4a86-b1f9-8bf783cc65aa-c000", 3);
    Assert.assertEquals(3, allocateNodes.size());
    for (ConsistentHash.Node node : allocateNodes) {
      Assert.assertTrue(consistentHash.contains(node));
    }

    Set<ConsistentHash.Node> allocateAllNodes =
        consistentHash.allocateNodes("part-00000-38af6778-964a-4a86-b1f9-8bf783cc65aa-c000", 11);
    Assert.assertEquals(10, allocateAllNodes.size());
    for (ConsistentHash.Node node : allocateAllNodes) {
      Assert.assertTrue(nodes.contains(node));
    }
  }

  private static class HostNode implements ConsistentHash.Node {
    private final String host;

    HostNode(String host) {
      this.host = host;
    }

    @Override
    public String key() {
      return host;
    }

    @Override
    public String toString() {
      return host;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(host);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof HostNode)) return false;
      HostNode that = (HostNode) o;
      return Objects.equals(host, that.host);
    }
  }
}
```

---

# heuristic

*This section contains 6 source files.*

## AddFallbackTags.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/AddFallbackTags.scala`

```scala
package org.apache.gluten.extension.columnar.heuristic

// Add fallback tags when validator returns negative outcome.
case class AddFallbackTags(validator: Validator) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp {
      case p if FallbackTags.maybeOffloadable(p) => addFallbackTag(p)
      case _ =>
    }
    plan
  }

  private def addFallbackTag(plan: SparkPlan): Unit = {
    val outcome = validator.validate(plan)
    outcome match {
      case Validator.Failed(reason) =>
        FallbackTags.add(plan, reason)
      case Validator.Passed =>
    }
  }
}

object AddFallbackTags {}
```

---

## FallbackNode.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/FallbackNode.scala`

```scala
package org.apache.gluten.extension.columnar.heuristic

/** A wrapper to specify the plan is fallback plan, the caller side should unwrap it. */
case class FallbackNode(fallbackPlan: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = fallbackPlan.output
}
```

---

## HeuristicApplier.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/HeuristicApplier.scala`

```scala
package org.apache.gluten.extension.columnar.heuristic

/**
 * Columnar rule applier that optimizes, implements Spark plan into Gluten plan by heuristically
 * applying columnar rules in fixed order.
 */
class HeuristicApplier(
    session: SparkSession,
    transformBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    fallbackPolicyBuilders: Seq[ColumnarRuleCall => SparkPlan => Rule[SparkPlan]],
    postBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    finalBuilders: Seq[ColumnarRuleCall => Rule[SparkPlan]],
    ruleWrappers: Seq[Rule[SparkPlan] => Rule[SparkPlan]])
  extends ColumnarRuleApplier
  with Logging
  with LogLevelUtil {
  override def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val call = new ColumnarRuleCall(session, CallerInfo.create(), outputsColumnar)
    makeRule(call).apply(plan)
  }

  private def makeRule(call: ColumnarRuleCall): Rule[SparkPlan] = {
    originalPlan =>
      val suggestedPlan = transformPlan("transform", transformRules(call), originalPlan)
      val finalPlan = transformPlan(
        "fallback",
        fallbackPolicies(call).map(_(originalPlan)),
        suggestedPlan) match {
        case FallbackNode(fallbackPlan) =>
          // we should use vanilla c2r rather than native c2r,
          // and there should be no `GlutenPlan` anymore,
          // so skip the `postRules()`.
          fallbackPlan
        case plan =>
          transformPlan("post", postRules(call), plan)
      }
      transformPlan("final", finalRules(call), finalPlan)
  }

  private def transformPlan(
      phase: String,
      rules: Seq[Rule[SparkPlan]],
      plan: SparkPlan): SparkPlan = {
    val wrappedRules = ruleWrappers.foldLeft(rules) {
      case (rules, wrapper) =>
        rules.map(wrapper)
    }
    new ColumnarRuleExecutor(phase, wrappedRules).execute(plan)
  }

  /**
   * Rules to let planner create a suggested Gluten plan being sent to `fallbackPolicies` in which
   * the plan will be breakdown and decided to be fallen back or not.
   */
  private def transformRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    transformBuilders.map(b => b.apply(call))
  }

  /**
   * Rules to add wrapper `FallbackNode`s on top of the input plan, as hints to make planner fall
   * back the whole input plan to the original vanilla Spark plan.
   */
  private def fallbackPolicies(call: ColumnarRuleCall): Seq[SparkPlan => Rule[SparkPlan]] = {
    fallbackPolicyBuilders.map(b => b.apply(call))
  }

  /**
   * Rules applying to non-fallen-back Gluten plans. To do some post cleanup works on the plan to
   * make sure it be able to run and be compatible with Spark's execution engine.
   */
  private def postRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    postBuilders.map(b => b.apply(call))
  }

  /*
   * Rules consistently applying to all input plans after all other rules have been applied, despite
   * whether the input plan is fallen back or not.
   */
  private def finalRules(call: ColumnarRuleCall): Seq[Rule[SparkPlan]] = {
    finalBuilders.map(b => b.apply(call))
  }
}

object HeuristicApplier {}
```

---

## HeuristicTransform.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/HeuristicTransform.scala`

```scala
package org.apache.gluten.extension.columnar.heuristic

/**
 * Rule to offload Spark query plan to Gluten query plan using programed heuristics.
 *
 * The procedure consists of 3 stages:
 *
 *   1. Rewrite 2. Validate 3. Offload
 *
 * In the rewrite stage, planner will try converting the Spark query plan to various forms of
 * possible alternative Spark query plans, then choose the optimal one to send to next stage. During
 * which, the same validation code that is about to be used in stage 2 might be invoked early to
 * predict on the estimate "cost" of an alternative Spark query plan.
 *
 * Once the plan is rewritten, query planner will call native validation code in stage 2 to
 * determine which part of the plan is offload-able or not, then add fallback tags to the
 * non-offload-able parts.
 *
 * In stage 3, query planner will convert the offload-able Spark plan nodes into Gluten plan nodes.
 */
class HeuristicTransform private (all: Seq[Rule[SparkPlan]])
  extends Rule[SparkPlan]
  with LogLevelUtil {
  override def apply(plan: SparkPlan): SparkPlan = {
    all.foldLeft(plan) {
      case (plan, single) =>
        single(plan)
    }
  }
}

object HeuristicTransform {
  def withRules(all: Seq[Rule[SparkPlan]]): HeuristicTransform = {
    new HeuristicTransform(all)
  }

  /**
   * A simple heuristic transform rule with a validator and some offload rules.
   *
   * Validator will be called before applying the offload rules.
   */
  case class Simple(validator: Validator, offloadRules: Seq[OffloadSingleNode])
    extends Rule[SparkPlan]
    with Logging {
    override def apply(plan: SparkPlan): SparkPlan = {
      offloadRules.foldLeft(plan) {
        case (p, rule) =>
          p.transformUp {
            node =>
              validator.validate(node) match {
                case Validator.Passed =>
                  rule.offload(node)
                case Validator.Failed(reason) =>
                  logDebug(s"Validation failed by reason: $reason on query plan: ${node.nodeName}")
                  if (FallbackTags.maybeOffloadable(node)) {
                    FallbackTags.add(node, reason)
                  }
                  node
              }
          }
      }
    }
  }

  /**
   * A heuristic transform rule with given rewrite rules. Fallback tags will be used in the
   * procedure to determine which part of the plan is or is not eligible to be offloaded. The tags
   * should also be correctly handled in the offload rules.
   *
   * TODO: Handle tags internally. Remove tag handling code in user offload rules.
   */
  case class WithRewrites(
      validator: Validator,
      rewriteRules: Seq[RewriteSingleNode],
      offloadRules: Seq[OffloadSingleNode])
    extends Rule[SparkPlan] {
    private val validate = AddFallbackTags(validator)
    private val rewrite = RewriteSparkPlanRulesManager(validate, rewriteRules)
    private val offload = LegacyOffload(offloadRules)

    override def apply(plan: SparkPlan): SparkPlan = {
      Seq(rewrite, validate, offload).foldLeft(plan) {
        case (plan, stage) =>
          stage(plan)
      }
    }
  }

  // Creates a static HeuristicTransform rule for use in certain
  // places that requires to emulate the offloading of a Spark query plan.
  //
  // TODO: Avoid using this and eventually remove the API.
  def static(): HeuristicTransform = {
    val exts = new SparkSessionExtensions()
    val dummyInjector = new Injector(exts)
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    Component.sorted().reverse.foreach(_.injectRules(dummyInjector))
    val session = SparkSession.getActiveSession.getOrElse(
      throw new GlutenException(
        "HeuristicTransform#static can only be called when an active Spark session exists"))
    val call = new ColumnarRuleCall(session, CallerInfo.create(), false)
    dummyInjector.gluten.legacy.createHeuristicTransform(call)
  }
}
```

---

## LegacyOffload.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/LegacyOffload.scala`

```scala
package org.apache.gluten.extension.columnar.heuristic

class LegacyOffload(rules: Seq[OffloadSingleNode]) extends Rule[SparkPlan] with LogLevelUtil {
  def apply(plan: SparkPlan): SparkPlan = {
    val out =
      rules.foldLeft(plan)((p, rule) => p.transformUp { case p => rule.offload(p) })
    out
  }
}

object LegacyOffload {
  def apply(rules: Seq[OffloadSingleNode]): LegacyOffload = {
    new LegacyOffload(rules)
  }
}
```

---

## RewriteSparkPlanRulesManager.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/heuristic/RewriteSparkPlanRulesManager.scala`

```scala
package org.apache.gluten.extension.columnar.heuristic

case class RewrittenNodeWall(originalChild: SparkPlan) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def supportsColumnar: Boolean = originalChild.supportsColumnar
  override def output: Seq[Attribute] = originalChild.output
  override def outputOrdering: Seq[SortOrder] = originalChild.outputOrdering
  override def outputPartitioning: Partitioning = originalChild.outputPartitioning
}

/**
 * A rule that holds a batch of [[Rule]]s to rewrite spark plan. When an operator can not be
 * offloaded to native, we try to rewrite it, e.g., pull out the complex exprs, so that we have one
 * more chance to offload it. If the rewritten plan still can not be offloaded, fallback to origin.
 *
 * Note that, this rule does not touch and tag these operators who does not need to rewrite.
 */
class RewriteSparkPlanRulesManager private (
    validateRule: Rule[SparkPlan],
    rewriteRules: Seq[RewriteSingleNode])
  extends Rule[SparkPlan] {

  private def mayNeedRewrite(plan: SparkPlan): Boolean = {
    FallbackTags.maybeOffloadable(plan) && rewriteRules.exists(_.isRewritable(plan))
  }

  private def getFallbackTagBack(rewrittenPlan: SparkPlan): Option[FallbackTag] = {
    // The rewritten plan may contain more nodes than origin, for now it should only be
    // `ProjectExec`.
    // TODO: Find a better approach than checking `p.isInstanceOf[ProjectExec]` which is not
    //  general.
    val target = rewrittenPlan.collect {
      case p if !p.isInstanceOf[ProjectExec] && !p.isInstanceOf[RewrittenNodeWall] => p
    }
    assert(target.size == 1)
    FallbackTags.getOption(target.head)
  }

  private def applyRewriteRules(origin: SparkPlan): (SparkPlan, Option[String]) = {
    try {
      val rewrittenPlan = rewriteRules.foldLeft(origin) {
        case (plan, rule) =>
          // Some rewrite rules may generate new parent plan node, we should use transform to
          // rewrite the original plan. For example, PullOutPreProject and PullOutPostProject
          // will generate post-project plan node.
          plan.transformUp { case p => rule.rewrite(p) }
      }
      (rewrittenPlan, None)
    } catch {
      case e: Exception =>
        // TODO: Remove this catch block
        //  See https://github.com/apache/incubator-gluten/issues/7766
        (origin, Option(e.getMessage))
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case origin if mayNeedRewrite(origin) =>
        // Add a wall to avoid transforming unnecessary nodes.
        val withWall = origin.mapChildren(RewrittenNodeWall)
        val (rewrittenPlan, error) = applyRewriteRules(withWall)
        if (error.isDefined) {
          // Return origin if there is an exception during rewriting rules.
          // Note, it is not expected, but it happens in CH backend when pulling out
          // aggregate.
          // TODO: Fix the exception and remove this branch
          FallbackTags.add(origin, error.get)
          origin
        } else if (withWall.fastEquals(rewrittenPlan)) {
          // Return origin if the rewrite rules do nothing.
          // We do not add tag and leave it to the outside `AddFallbackTagRule`.
          origin
        } else {
          validateRule.apply(rewrittenPlan)
          val tag = getFallbackTagBack(rewrittenPlan)
          if (tag.isDefined) {
            // If the rewritten plan is still not transformable, return the original plan.
            FallbackTags.add(origin, tag.get)
            origin
          } else {
            rewrittenPlan.transformUp {
              case wall: RewrittenNodeWall => wall.originalChild
              case p if p.logicalLink.isEmpty =>
                // Add logical link to pull out project to make fallback reason work,
                // see `GlutenFallbackReporter`.
                origin.logicalLink.foreach(p.setLogicalLink)
                p
            }
          }
        }
    }
  }
}

object RewriteSparkPlanRulesManager {
  def apply(
      validateRule: Rule[SparkPlan],
      rewriteRules: Seq[RewriteSingleNode]): Rule[SparkPlan] = {
    new RewriteSparkPlanRulesManager(validateRule, rewriteRules)
  }
}
```

---

# initializer

*This section contains 1 source files.*

## CodedInputStreamClassInitializer.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/initializer/CodedInputStreamClassInitializer.scala`

```scala
package org.apache.gluten.initializer

/**
 * Pre-load the class instance for CodedInputStream and modify its defaultRecursionLimit to avoid
 * the limit is hit for deeply nested plans. This is based on the fact that the same class loader is
 * used to load this class in the program, then this modification will really take effect.
 */
object CodedInputStreamClassInitializer extends Logging {
  private val newDefaultRecursionLimit = 100000

  def modifyDefaultRecursionLimitUnsafe: Unit = {
    try {
      // scalastyle:off classforname
      val clazz: Class[_] =
        try {
          // Use the shaded class name.
          Class.forName("org.apache.gluten.shaded.com.google.protobuf.CodedInputStream")
        } catch {
          // The above class is shaded in final package phase (see package/pom.xml).
          // If ClassNotFoundException happens, e.g., in mvn test, load the original class instead.
          case _: ClassNotFoundException =>
            Class.forName("com.google.protobuf.CodedInputStream")
        }
      // scalastyle:on classforname
      val field: Field = clazz.getDeclaredField("defaultRecursionLimit")
      field.setAccessible(true)
      // Enlarge defaultRecursionLimit whose original value is 100.
      field.setInt(null, newDefaultRecursionLimit)
      logInfo(
        s"The defaultRecursionLimit in protobuf has been increased to $newDefaultRecursionLimit")
    } catch {
      case e: Exception =>
        log.error("Failed to modify the DefaultRecursionLimit in protobuf: " + e.getMessage)
    }
  }
}
```

---

# injector

*This section contains 5 source files.*

## GlutenInjector.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/injector/GlutenInjector.scala`

```scala
package org.apache.gluten.extension.injector

/** Injector used to inject query planner rules into Gluten. */
class GlutenInjector private[injector] (control: InjectorControl) {
  val legacy: LegacyInjector = new LegacyInjector()
  val ras: RasInjector = new RasInjector()

  private[injector] def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(
      control.disabler().wrapColumnarRule(s => new GlutenColumnarRule(s, applier)))
  }

  private def applier(session: SparkSession): ColumnarRuleApplier = {
    val conf = new GlutenCoreConfig(session.sessionState.conf)
    if (conf.enableRas) {
      return ras.createApplier(session)
    }
    legacy.createApplier(session)
  }
}

object GlutenInjector {
  class LegacyInjector {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val transformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val fallbackPolicyBuilders =
      mutable.Buffer.empty[ColumnarRuleCall => SparkPlan => Rule[SparkPlan]]
    private val postBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val finalBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val ruleWrappers = mutable.Buffer.empty[Rule[SparkPlan] => Rule[SparkPlan]]

    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }

    def injectTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      transformBuilders += builder
    }

    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }

    def injectFallbackPolicy(builder: ColumnarRuleCall => SparkPlan => Rule[SparkPlan]): Unit = {
      fallbackPolicyBuilders += builder
    }

    def injectPost(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postBuilders += builder
    }

    def injectFinal(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      finalBuilders += builder
    }

    def injectRuleWrapper(wrapper: Rule[SparkPlan] => Rule[SparkPlan]): Unit = {
      ruleWrappers += wrapper
    }

    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new HeuristicApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createHeuristicTransform(c)) ++ postTransformBuilders).toSeq,
        fallbackPolicyBuilders.toSeq,
        postBuilders.toSeq,
        finalBuilders.toSeq,
        ruleWrappers.toSeq
      )
    }

    def createHeuristicTransform(call: ColumnarRuleCall): HeuristicTransform = {
      val all = transformBuilders.map(_(call))
      HeuristicTransform.withRules(all.toSeq)
    }
  }

  class RasInjector extends Logging {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val rasRuleBuilders = mutable.Buffer.empty[ColumnarRuleCall => RasRule[SparkPlan]]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val ruleWrappers = mutable.Buffer.empty[Rule[SparkPlan] => Rule[SparkPlan]]

    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }

    def injectRasRule(builder: ColumnarRuleCall => RasRule[SparkPlan]): Unit = {
      rasRuleBuilders += builder
    }

    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }

    def injectRuleWrapper(wrapper: Rule[SparkPlan] => Rule[SparkPlan]): Unit = {
      ruleWrappers += wrapper
    }

    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new EnumeratedApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createEnumeratedTransform(c)) ++ postTransformBuilders).toSeq,
        ruleWrappers.toSeq)
    }

    def createEnumeratedTransform(call: ColumnarRuleCall): EnumeratedTransform = {
      // Build RAS rules.
      val rules = rasRuleBuilders.map(_(call))
      val costModel = GlutenCostModel.find(new GlutenCoreConfig(call.sqlConf).rasCostModel)
      // Create transform.
      EnumeratedTransform(costModel, rules.toSeq)
    }
  }
}
```

---

## Injector.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/injector/Injector.scala`

```scala
package org.apache.gluten.extension.injector

/** Injector used to inject extensible components into Spark and Gluten. */
class Injector(extensions: SparkSessionExtensions) {
  val control = new InjectorControl()
  val spark: SparkInjector = new SparkInjector(control, extensions)
  val gluten: GlutenInjector = new GlutenInjector(control)

  private[extension] def inject(): Unit = {
    // The regular Spark rules already injected with the `injectRules` of `RuleApi` directly.
    // Only inject the Spark columnar rule here.
    gluten.inject(extensions)
  }
}
```

---

## InjectorControl.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/injector/InjectorControl.scala`

```scala
package org.apache.gluten.extension.injector

class InjectorControl private[injector] () {
  private val disablerBuffer: mutable.ListBuffer[Disabler] =
    mutable.ListBuffer()
  private var combined: Disabler = (_: SparkSession) => false

  def disableOn(one: Disabler): Unit = synchronized {
    disablerBuffer += one
    // Update the combined disabler.
    val disablerList = disablerBuffer.toList
    combined = s => disablerList.exists(_.disabled(s))
  }

  private[injector] def disabler(): Disabler = synchronized {
    combined
  }
}

object InjectorControl {
  trait Disabler {
    // If true, the injected rule will be disabled.
    protected[injector] def disabled(session: SparkSession): Boolean
  }

  private object Disabler {
    implicit private[injector] class DisablerOps(disabler: Disabler) {
      def wrapRule[TreeType <: TreeNode[_]](
          ruleBuilder: SparkSession => Rule[TreeType]): SparkSession => Rule[TreeType] = session =>
        {
          val rule = ruleBuilder(session)
          new Rule[TreeType] with DisablerAware {
            override val ruleName: String = rule.ruleName
            override def apply(plan: TreeType): TreeType = {
              if (disabler.disabled(session)) {
                return plan
              }
              rule(plan)
            }
          }
        }

      def wrapStrategy(strategyBuilder: StrategyBuilder): StrategyBuilder = session => {
        val strategy = strategyBuilder(session)
        new Strategy with DisablerAware {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
            if (disabler.disabled(session)) {
              return Nil
            }
            strategy(plan)
          }
        }
      }

      def wrapParser(parserBuilder: ParserBuilder): ParserBuilder = (session, parser) => {
        val before = parser
        val after = parserBuilder(session, before)
        // Use dynamic proxy to get rid of 3.2 compatibility issues.
        java.lang.reflect.Proxy
          .newProxyInstance(
            classOf[ParserInterface].getClassLoader,
            Array(classOf[ParserInterface], classOf[DisablerAware]),
            new InvocationHandler {
              override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
                try {
                  if (disabler.disabled(session)) {
                    return method.invoke(before, args: _*)
                  }
                  method.invoke(after, args: _*)
                } catch {
                  case e: InvocationTargetException =>
                    // Unwrap the ITE.
                    throw e.getCause
                }
              }
            }
          )
          .asInstanceOf[ParserInterface]
      }

      def wrapFunction(functionDescription: FunctionDescription): FunctionDescription = {
        val (identifier, info, builder) = functionDescription
        val wrappedBuilder: FunctionBuilder = new FunctionBuilder with DisablerAware {
          override def apply(children: Seq[Expression]): Expression = {
            if (
              disabler.disabled(SparkSession.getActiveSession.getOrElse(
                throw new IllegalStateException("Active Spark session not found")))
            ) {
              throw new UnsupportedOperationException(
                s"Function ${info.getName} is not callable as Gluten is disabled")
            }
            builder(children)
          }
        }
        (identifier, info, wrappedBuilder)
      }

      def wrapColumnarRule(columnarRuleBuilder: ColumnarRuleBuilder): ColumnarRuleBuilder =
        session => {
          val columnarRule = columnarRuleBuilder(session)
          new ColumnarRule with DisablerAware {
            override val preColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.preColumnarTransitions.apply(plan)
                }
              }
            }

            override val postColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.postColumnarTransitions.apply(plan)
                }
              }
            }
          }
        }
    }
  }

  /**
   * The entity (could be a rule, a parser, cost evaluator) that is dynamically injected to Spark,
   * whose effectivity is under the control by a disabler.
   */
  trait DisablerAware
}
```

---

## SparkInjector.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/injector/SparkInjector.scala`

```scala
package org.apache.gluten.extension.injector

/** Injector used to inject query planner rules into Spark. */
class SparkInjector private[injector] (
    control: InjectorControl,
    extensions: SparkSessionExtensions) {
  def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
    extensions.injectQueryStagePrepRule(control.disabler().wrapRule(builder))
  }

  def injectResolutionRule(builder: RuleBuilder): Unit = {
    extensions.injectResolutionRule(control.disabler().wrapRule(builder))
  }

  def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
    extensions.injectPostHocResolutionRule(control.disabler().wrapRule(builder))
  }

  def injectOptimizerRule(builder: RuleBuilder): Unit = {
    extensions.injectOptimizerRule(control.disabler().wrapRule(builder))
  }

  def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
    extensions.injectPlannerStrategy(control.disabler().wrapStrategy(builder))
  }

  def injectParser(builder: ParserBuilder): Unit = {
    extensions.injectParser(control.disabler().wrapParser(builder))
  }

  def injectFunction(functionDescription: FunctionDescription): Unit = {
    extensions.injectFunction(control.disabler().wrapFunction(functionDescription))
  }

  def injectPreCBORule(builder: RuleBuilder): Unit = {
    extensions.injectPreCBORule(control.disabler().wrapRule(builder))
  }
}
```

---

## package.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/injector/package.scala`

```scala
package org.apache.gluten.extension

package object injector {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]
  type StrategyBuilder = SparkSession => Strategy
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
  type QueryStagePrepRuleBuilder = SparkSession => Rule[SparkPlan]
  type ColumnarRuleBuilder = SparkSession => ColumnarRule
}
```

---

# internal

*This section contains 4 source files.*

## ConfigProvider.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/internal/ConfigProvider.scala`

```scala
package org.apache.spark.sql.internal

/** A source of configuration values. */
trait GlutenConfigProvider {
  def get(key: String): Option[String]
}

class SQLConfProvider(conf: SQLConf) extends GlutenConfigProvider {
  override def get(key: String): Option[String] = Option(conf.settings.get(key))
}

class MapProvider(conf: Map[String, String]) extends GlutenConfigProvider {
  override def get(key: String): Option[String] = conf.get(key)
}
```

---

## GlutenConfigUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/internal/GlutenConfigUtil.scala`

```scala
package org.apache.spark.sql.internal

object GlutenConfigUtil {
  private def getConfString(
      configProvider: GlutenConfigProvider,
      key: String,
      value: String): String = {
    ConfigRegistry
      .findEntry(key)
      .map {
        _.readFrom(configProvider) match {
          case o: Option[_] => o.map(_.toString).getOrElse(value)
          case null => value
          case v => v.toString
        }
      }
      .getOrElse(value)
  }

  def parseConfig(conf: Map[String, String]): Map[String, String] = {
    val provider = new MapProvider(conf.filter(_._1.startsWith("spark.gluten.")))
    conf.map {
      case (k, v) =>
        if (k.startsWith("spark.gluten.")) {
          (k, getConfString(provider, k, v))
        } else {
          (k, v)
        }
    }.toMap
  }

  def mapByteConfValue(conf: Map[String, String], key: String, unit: ByteUnit)(
      f: Long => Unit): Unit = {
    conf.get(key).foreach(v => f(JavaUtils.byteStringAs(v, unit)))
  }
}
```

---

## SparkConfigUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/sql/internal/SparkConfigUtil.scala`

```scala
package org.apache.spark.sql.internal

object SparkConfigUtil {

  implicit class RichSparkConf(val conf: SparkConf) {
    def get[T](entry: SparkConfigEntry[T]): T = {
      SparkConfigUtil.get(conf, entry)
    }

    def get[T](entry: ConfigEntry[T]): T = {
      SparkConfigUtil.get(conf, entry)
    }

    def set[T](entry: SparkConfigEntry[T], value: T): SparkConf = {
      SparkConfigUtil.set(conf, entry, value)
    }

    def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
      SparkConfigUtil.set(conf, entry, value)
    }

    def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
      SparkConfigUtil.set(conf, entry, value)
    }
  }

  def get[T](conf: SparkConf, entry: SparkConfigEntry[T]): T = {
    conf.get(entry)
  }

  def get[T](conf: SparkConf, entry: ConfigEntry[T]): T = {
    conf
      .getOption(entry.key)
      .map(entry.valueConverter)
      .getOrElse(entry.defaultValue.getOrElse(None).asInstanceOf[T])
  }

  def get[T](conf: java.util.Map[String, String], entry: SparkConfigEntry[T]): T = {
    Option(conf.get(entry.key))
      .map(entry.valueConverter)
      .getOrElse(entry.defaultValue.getOrElse(None).asInstanceOf[T])
  }

  def get[T](conf: java.util.Map[String, String], entry: ConfigEntry[T]): T = {
    Option(conf.get(entry.key))
      .map(entry.valueConverter)
      .getOrElse(entry.defaultValue.getOrElse(None).asInstanceOf[T])
  }

  def set[T](conf: SparkConf, entry: SparkConfigEntry[T], value: T): SparkConf = {
    conf.set(entry, value)
  }

  def set[T](conf: SparkConf, entry: OptionalConfigEntry[T], value: T): SparkConf = {
    conf.set(entry, value)
  }

  def set[T](conf: SparkConf, entry: ConfigEntry[T], value: T): SparkConf = {
    value match {
      case Some(v) => conf.set(entry.key, v.toString)
      case None | null => conf.set(entry.key, null)
      case _ => conf.set(entry.key, value.toString)
    }
  }
}
```

---

## GlutenConfigUtilSuite.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/spark/sql/internal/GlutenConfigUtilSuite.scala`

```scala
package org.apache.spark.sql.internal

class GlutenConfigUtilSuite extends AnyFunSuite {

  test("mapByteConfValue should return correct value") {
    val conf = Map(
      "spark.unsafe.sorter.spill.reader.buffer.size" -> "2m"
    )

    GlutenConfigUtil.mapByteConfValue(
      conf,
      "spark.unsafe.sorter.spill.reader.buffer.size",
      ByteUnit.BYTE)(v => assert(2097152L.equals(v)))
  }
}
```

---

# iterator

*This section contains 5 source files.*

## ClosableIterator.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/iterator/ClosableIterator.java`

```java
package org.apache.gluten.iterator;

public abstract class ClosableIterator<T> implements AutoCloseable, Serializable, Iterator<T> {
  protected final AtomicBoolean closed = new AtomicBoolean(false);

  public ClosableIterator() {}

  @Override
  public final boolean hasNext() {
    if (closed.get()) {
      throw new GlutenException("Iterator has been closed.");
    }
    try {
      return hasNext0();
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public final T next() {
    if (closed.get()) {
      throw new GlutenException("Iterator has been closed.");
    }
    try {
      return next0();
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public final void close() {
    if (closed.compareAndSet(false, true)) {
      close0();
    }
  }

  protected abstract void close0();

  protected abstract boolean hasNext0() throws Exception;

  protected abstract T next0() throws Exception;
}
```

---

## Iterators.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/iterator/Iterators.scala`

```scala
package org.apache.gluten.iterator

/**
 * Utility class to provide iterator wrappers for non-trivial use cases. E.g. iterators that manage
 * payload's lifecycle.
 */
object Iterators {
  sealed trait Version
  case object V1 extends Version

  private val DEFAULT_VERSION: Version = V1

  trait WrapperBuilder[A] {
    def recyclePayload(closeCallback: (A) => Unit): WrapperBuilder[A]
    def recycleIterator(completionCallback: => Unit): WrapperBuilder[A]
    def collectLifeMillis(onCollected: Long => Unit): WrapperBuilder[A]
    def collectReadMillis(onAdded: Long => Unit): WrapperBuilder[A]
    def asInterruptible(context: TaskContext): WrapperBuilder[A]
    def protectInvocationFlow(): WrapperBuilder[A]
    def create(): Iterator[A]
  }

  def wrap[A](in: Iterator[A]): WrapperBuilder[A] = {
    wrap(DEFAULT_VERSION, in)
  }

  def wrap[A](version: Version, in: Iterator[A]): WrapperBuilder[A] = {
    version match {
      case V1 =>
        new WrapperBuilderV1[A](in)
    }
  }
}
```

---

## IteratorsV1.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/iterator/IteratorsV1.scala`

```scala
package org.apache.gluten.iterator

object IteratorsV1 {
  private class PayloadCloser[A](in: Iterator[A])(closeCallback: A => Unit) extends Iterator[A] {
    private val _none = new Object
    private var _prev: Any = _none

    TaskResources.addRecycler("Iterators#PayloadCloser", 100) {
      tryClose()
    }

    override def hasNext: Boolean = {
      tryClose()
      in.hasNext
    }

    override def next(): A = {
      val a: A = in.next()
      this.synchronized {
        _prev = a
      }
      a
    }

    private def tryClose(): Unit = {
      this.synchronized {
        if (_prev != _none) closeCallback.apply(_prev.asInstanceOf[A])
        _prev = _none // make sure the payload is closed once
      }
    }
  }

  private class IteratorCompleter[A](in: Iterator[A])(completionCallback: => Unit)
    extends Iterator[A] {
    private val completed = new AtomicBoolean(false)

    TaskResources.addRecycler("Iterators#IteratorRecycler", 100) {
      tryComplete()
    }

    override def hasNext: Boolean = {
      val out = in.hasNext
      if (!out) {
        tryComplete()
      }
      out
    }

    override def next(): A = {
      in.next()
    }

    private def tryComplete(): Unit = {
      if (!completed.compareAndSet(false, true)) {
        return // make sure the iterator is completed once
      }
      completionCallback
    }
  }

  private class LifeTimeAccumulator[A](in: Iterator[A], onCollected: Long => Unit)
    extends Iterator[A] {
    private val closed = new AtomicBoolean(false)
    private val startTime = System.nanoTime()

    TaskResources.addRecycler("Iterators#LifeTimeAccumulator", 100) {
      tryFinish()
    }

    override def hasNext: Boolean = {
      val out = in.hasNext
      if (!out) {
        tryFinish()
      }
      out
    }

    override def next(): A = {
      in.next()
    }

    private def tryFinish(): Unit = {
      // pipeline metric should only be calculate once.
      if (!closed.compareAndSet(false, true)) {
        return
      }
      val lifeTime = TimeUnit.NANOSECONDS.toMillis(
        System.nanoTime() - startTime
      )
      onCollected(lifeTime)
    }
  }

  private class ReadTimeAccumulator[A](in: Iterator[A], onAdded: Long => Unit) extends Iterator[A] {

    override def hasNext: Boolean = {
      val prev = System.nanoTime()
      val out = in.hasNext
      val after = System.nanoTime()
      val duration = TimeUnit.NANOSECONDS.toMillis(after - prev)
      onAdded(duration)
      out
    }

    override def next(): A = {
      val prev = System.nanoTime()
      val out = in.next()
      val after = System.nanoTime()
      val duration = TimeUnit.NANOSECONDS.toMillis(after - prev)
      onAdded(duration)
      out
    }
  }

  /**
   * To protect the wrapped iterator to avoid undesired order of calls to its `hasNext` and `next`
   * methods.
   */
  private class InvocationFlowProtection[A](in: Iterator[A]) extends Iterator[A] {
    sealed private trait State
    private case object Init extends State
    private case class HasNextCalled(hasNext: Boolean) extends State
    private case object NextCalled extends State

    private var state: State = Init

    override def hasNext: Boolean = {
      val out = state match {
        case Init | NextCalled =>
          in.hasNext
        case HasNextCalled(lastHasNext) =>
          lastHasNext
      }
      state = HasNextCalled(out)
      out
    }

    override def next(): A = {
      val out = state match {
        case Init | NextCalled =>
          if (!in.hasNext) {
            throw new IllegalStateException("End of stream")
          }
          in.next()
        case HasNextCalled(lastHasNext) =>
          if (!lastHasNext) {
            throw new IllegalStateException("End of stream")
          }
          in.next()
      }
      state = NextCalled
      out
    }
  }

  class WrapperBuilderV1[A] private[iterator] (in: Iterator[A]) extends WrapperBuilder[A] {
    private var wrapped: Iterator[A] = in

    override def recyclePayload(closeCallback: (A) => Unit): WrapperBuilder[A] = {
      wrapped = new PayloadCloser(wrapped)(closeCallback)
      this
    }

    override def recycleIterator(completionCallback: => Unit): WrapperBuilder[A] = {
      wrapped = new IteratorCompleter(wrapped)(completionCallback)
      this
    }

    override def collectLifeMillis(onCollected: Long => Unit): WrapperBuilder[A] = {
      wrapped = new LifeTimeAccumulator[A](wrapped, onCollected)
      this
    }

    override def collectReadMillis(onAdded: Long => Unit): WrapperBuilder[A] = {
      wrapped = new ReadTimeAccumulator[A](wrapped, onAdded)
      this
    }

    override def asInterruptible(context: TaskContext): WrapperBuilder[A] = {
      wrapped = new InterruptibleIterator[A](context, wrapped)
      this
    }

    override def protectInvocationFlow(): WrapperBuilder[A] = {
      wrapped = new InvocationFlowProtection[A](wrapped)
      this
    }

    override def create(): Iterator[A] = {
      wrapped
    }
  }
}
```

---

## IteratorSuite.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/gluten/iterator/IteratorSuite.scala`

```scala
package org.apache.gluten.iterator

class IteratorV1Suite extends IteratorSuite {
  override protected def wrap[A](in: Iterator[A]): WrapperBuilder[A] = Iterators.wrap(V1, in)
}

abstract class IteratorSuite extends AnyFunSuite {
  protected def wrap[A](in: Iterator[A]): WrapperBuilder[A]

  test("Trivial wrapping") {
    val strings = Array[String]("one", "two", "three")
    val itr = strings.toIterator
    val wrapped = wrap(itr)
      .create()
    assertResult(strings) {
      wrapped.toArray
    }
  }

  test("Complete iterator") {
    var completeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val wrapped = wrap(itr)
        .recycleIterator {
          completeCount += 1
        }
        .create()
      assertResult(strings) {
        wrapped.toArray
      }
      assert(completeCount == 1)
    }
    assert(completeCount == 1)
  }

  test("Complete intermediate iterator") {
    var completeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val _ = wrap(itr)
        .recycleIterator {
          completeCount += 1
        }
        .create()
      assert(completeCount == 0)
    }
    assert(completeCount == 1)
  }

  test("Close payload") {
    var closeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val wrapped = wrap(itr)
        .recyclePayload { _: String => closeCount += 1 }
        .create()
      assertResult(strings) {
        wrapped.toArray
      }
      assert(closeCount == 3)
    }
    assert(closeCount == 3)
  }

  test("Close intermediate payload") {
    var closeCount = 0
    TaskResources.runUnsafe {
      val strings = Array[String]("one", "two", "three")
      val itr = strings.toIterator
      val wrapped = wrap(itr)
        .recyclePayload { _: String => closeCount += 1 }
        .create()
      assertResult(strings.take(2)) {
        wrapped.take(2).toArray
      }
      assert(closeCount == 1) // the first one is closed after consumed
    }
    assert(closeCount == 2) // the second one is closed on task exit
  }

  test("Protect invocation flow") {
    var hasNextCallCount = 0
    var nextCallCount = 0
    val itr = new Iterator[Any] {
      override def hasNext: Boolean = {
        hasNextCallCount += 1
        true
      }

      override def next(): Any = {
        nextCallCount += 1
        new Object
      }
    }
    val wrapped = wrap(itr)
      .protectInvocationFlow()
      .create()
    wrapped.hasNext
    assert(hasNextCallCount == 1)
    assert(nextCallCount == 0)
    wrapped.hasNext
    assert(hasNextCallCount == 1)
    assert(nextCallCount == 0)
    wrapped.next
    assert(hasNextCallCount == 1)
    assert(nextCallCount == 1)
    wrapped.next
    assert(hasNextCallCount == 2)
    assert(nextCallCount == 2)
  }
}
```

---

## IteratorBenchmark.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/spark/iterator/IteratorBenchmark.scala`

```scala
package org.apache.spark.iterator

object IteratorBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Iterator Nesting") {
      TaskResources.runUnsafe {
        val nPayloads: Int = 50000000 // 50 millions

        def makeScalaIterator: Iterator[Any] = {
          (0 until nPayloads).view.map { _: Int => new Object }.iterator
        }

        def compareIterator(name: String)(
            makeGlutenIterator: Iterators.Version => Iterator[Any]): Unit = {
          val benchmark = new Benchmark(name, nPayloads, output = output)
          benchmark.addCase("Scala Iterator") {
            _ =>
              val count = makeScalaIterator.count(_ => true)
              assert(count == nPayloads)
          }
          benchmark.addCase("Gluten Iterator V1") {
            _ =>
              val count = makeGlutenIterator(V1).count(_ => true)
              assert(count == nPayloads)
          }
          benchmark.run()
        }

        compareIterator("0 Levels Nesting") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .create()
        }
        compareIterator("1 Levels Nesting - read") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .collectReadMillis { _ => }
              .create()
        }
        compareIterator("5 Levels Nesting - read") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .create()
        }
        compareIterator("10 Levels Nesting - read") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .collectReadMillis { _ => }
              .create()
        }
        compareIterator("1 Levels Nesting - recycle") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .recycleIterator {}
              .create()
        }
        compareIterator("5 Levels Nesting - recycle") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .create()
        }
        compareIterator("10 Levels Nesting - recycle") {
          version =>
            Iterators
              .wrap(version, makeScalaIterator)
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .recycleIterator {}
              .create()
        }
      }
    }

    runBenchmark("Iterator Multi Threads") {
      val nPayloads: Int = 50000000 // 50 millions

      def makeScalaIterator: Iterator[Any] = {
        (0 until nPayloads).view.map { _: Int => new Object }.iterator
      }

      def compareMultiThreadsIterator(name: String, threads: Int = 3)(
          makeGlutenIterator: Iterators.Version => Iterator[Any]): Unit = {
        val benchmark = new Benchmark(name, nPayloads, output = output)
        benchmark.addCase("Scala Iterator") {
          _ =>
            val pool = ThreadUtils.newDaemonFixedThreadPool(threads, "ScalaIterator")
            for (_ <- 0 until threads) {
              pool.execute(
                () => {
                  TaskResources.runUnsafe {
                    val count = makeScalaIterator.count(_ => true)
                    assert(count == nPayloads)
                  }
                })
            }
            pool.shutdown()
            pool.awaitTermination(10, TimeUnit.SECONDS)
        }
        benchmark.addCase("Gluten Iterator V1") {
          _ =>
            val pool = ThreadUtils.newDaemonFixedThreadPool(threads, "GlutenIteratorV1")
            for (_ <- 0 until threads) {
              pool.execute(
                () => {
                  TaskResources.runUnsafe {
                    val count = makeGlutenIterator(V1).count(_ => true)
                    assert(count == nPayloads)
                  }
                })
            }
            pool.shutdown()
            pool.awaitTermination(10, TimeUnit.SECONDS)
        }
        benchmark.run()
      }

      compareMultiThreadsIterator("Multi Threads - recycle") {
        version =>
          var count = 0
          Iterators
            .wrap(version, makeScalaIterator)
            .recyclePayload(_ => count += 1)
            .recycleIterator(assert(count == nPayloads))
            .create()
      }
    }
  }
}
```

---

# jni

*This section contains 2 source files.*

## JniLibLoader.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/jni/JniLibLoader.java`

```java
package org.apache.gluten.jni;

public class JniLibLoader {
  private static final Logger LOG = LoggerFactory.getLogger(JniLibLoader.class);

  private static final Set<String> LOADED_LIBRARY_PATHS = new HashSet<>();

  private final String workDir;
  private final Set<String> loadedLibraries = new HashSet<>();

  JniLibLoader(String workDir) {
    this.workDir = workDir;
  }

  private static String toRealPath(String libPath) {
    String realPath = libPath;
    try {
      while (Files.isSymbolicLink(Paths.get(realPath))) {
        realPath = Files.readSymbolicLink(Paths.get(realPath)).toString();
      }
      LOG.info("Read real path {} for libPath {}", realPath, libPath);
      return realPath;
    } catch (Throwable th) {
      throw new GlutenException("Error to read real path for libPath: " + libPath, th);
    }
  }

  private static void loadFromPath0(String libPath) {
    libPath = toRealPath(libPath);
    if (LOADED_LIBRARY_PATHS.contains(libPath)) {
      LOG.debug("Library in path {} has already been loaded, skipping", libPath);
    } else {
      System.load(libPath);
      LOADED_LIBRARY_PATHS.add(libPath);
      LOG.info("Library {} has been loaded using path-loading method", libPath);
    }
  }

  public static synchronized void loadFromPath(String libPath) {
    final File file = new File(libPath);
    if (!file.isFile() || !file.exists()) {
      throw new GlutenException("library at path: " + libPath + " is not a file or does not exist");
    }
    loadFromPath0(file.getAbsolutePath());
  }

  public synchronized void load(String libPath) {
    try {
      if (loadedLibraries.contains(libPath)) {
        LOG.debug("Library {} has already been loaded, skipping", libPath);
        return;
      }
      File file = moveToWorkDir(workDir, libPath);
      loadWithLink(file.getAbsolutePath(), null);
      loadedLibraries.add(libPath);
      LOG.info("Successfully loaded library {}", libPath);
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }

  public synchronized void loadAndCreateLink(String libPath, String linkName) {
    try {
      if (loadedLibraries.contains(libPath)) {
        LOG.debug("Library {} has already been loaded, skipping", libPath);
      }
      File file = moveToWorkDir(workDir, libPath);
      loadWithLink(file.getAbsolutePath(), linkName);
      loadedLibraries.add(libPath);
      LOG.info("Successfully loaded library {}", libPath);
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }

  private File moveToWorkDir(String workDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(workDir, libraryToLoad);
    final Path libPath = Paths.get(workDir, libraryToLoad);
    if (Files.exists(libPath)) {
      Files.delete(libPath);
    }
    final File temp = libPath.toFile();
    if (!temp.getParentFile().exists()) {
      temp.getParentFile().mkdirs();
    }
    try (InputStream is = JniLibLoader.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      }
      try {
        Files.copy(is, temp.toPath());
      } catch (Exception e) {
        throw new GlutenException(e);
      }
    }
    return temp;
  }

  private void loadWithLink(String libPath, String linkName) throws IOException {
    loadFromPath0(libPath);
    LOG.info("Library {} has been loaded", libPath);
    if (linkName != null) {
      Path target = Paths.get(libPath);
      Path link = Paths.get(workDir, linkName);
      if (Files.exists(link)) {
        LOG.info("Symbolic link already exists for library {}, deleting", libPath);
        Files.delete(link);
      }
      Files.createSymbolicLink(link, target);
      LOG.info("Symbolic link {} created for library {}", link, libPath);
    }
  }
}
```

---

## JniWorkspace.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/jni/JniWorkspace.java`

```java
package org.apache.gluten.jni;

public class JniWorkspace {
  private static final Logger LOG = LoggerFactory.getLogger(JniWorkspace.class);
  private static final Map<String, JniWorkspace> INSTANCES = new ConcurrentHashMap<>();

  // Will be initialized by user code
  private static JniWorkspace DEFAULT_INSTANCE = null;
  private static final Object DEFAULT_INSTANCE_INIT_LOCK = new Object();

  // For debugging purposes only
  private static JniWorkspace DEBUG_INSTANCE = null;

  private final String workDir;
  private final JniLibLoader jniLibLoader;

  private JniWorkspace(String rootDir) {
    try {
      LOG.info("Creating JNI workspace in root directory {}", rootDir);
      Path root = Paths.get(rootDir);
      Path created = Files.createTempDirectory(root, "gluten-");
      this.workDir = created.toAbsolutePath().toString();
      this.jniLibLoader = new JniLibLoader(workDir);
      LOG.info("JNI workspace {} created in root directory {}", workDir, rootDir);
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  public static void enableDebug(String debugDir) {
    // Preserve the JNI libraries even after process exits.
    // This is useful for debugging native code if the debug symbols were embedded in
    // the libraries.
    synchronized (DEFAULT_INSTANCE_INIT_LOCK) {
      if (DEBUG_INSTANCE == null) {
        final File tempRoot =
            Paths.get(debugDir).resolve("gluten-jni-debug-" + UUID.randomUUID()).toFile();
        try {
          FileUtils.forceMkdir(tempRoot);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        DEBUG_INSTANCE = createOrGet(tempRoot.getAbsolutePath());
      }
      Preconditions.checkNotNull(DEBUG_INSTANCE);
      if (DEFAULT_INSTANCE == null) {
        DEFAULT_INSTANCE = DEBUG_INSTANCE;
      }
      Preconditions.checkNotNull(DEFAULT_INSTANCE);
      if (DEFAULT_INSTANCE != DEBUG_INSTANCE) {
        throw new IllegalStateException("Default instance is already set to a non-debug instance");
      }
    }
  }

  public static void initializeDefault(Supplier<String> rootDir) {
    synchronized (DEFAULT_INSTANCE_INIT_LOCK) {
      if (DEFAULT_INSTANCE == null) {
        DEFAULT_INSTANCE = createOrGet(rootDir.get());
      }
    }
  }

  public static JniWorkspace getDefault() {
    Preconditions.checkNotNull(DEFAULT_INSTANCE, "Not call initializeDefault yet");
    return DEFAULT_INSTANCE;
  }

  private static JniWorkspace createOrGet(String rootDir) {
    return INSTANCES.computeIfAbsent(rootDir, JniWorkspace::new);
  }

  public String getWorkDir() {
    return workDir;
  }

  public JniLibLoader libLoader() {
    return jniLibLoader;
  }
}
```

---

# logging

*This section contains 1 source files.*

## LogLevelUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/logging/LogLevelUtil.scala`

```scala
package org.apache.gluten.logging

trait LogLevelUtil { self: Logging =>

  protected def logOnLevel(level: String, msg: => String): Unit =
    level match {
      case "TRACE" => logTrace(msg)
      case "DEBUG" => logDebug(msg)
      case "INFO" => logInfo(msg)
      case "WARN" => logWarning(msg)
      case "ERROR" => logError(msg)
      case _ => logDebug(msg)
    }

  protected def logOnLevel(level: String, msg: => String, e: Throwable): Unit =
    level match {
      case "TRACE" => logTrace(msg, e)
      case "DEBUG" => logDebug(msg, e)
      case "INFO" => logInfo(msg, e)
      case "WARN" => logWarning(msg, e)
      case "ERROR" => logError(msg, e)
      case _ => logDebug(msg, e)
    }
}
```

---

# memory

*This section contains 6 source files.*

## MemoryUsageRecorder.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/MemoryUsageRecorder.java`

```java
package org.apache.gluten.memory;

public interface MemoryUsageRecorder extends MemoryUsageStatsBuilder {
  void inc(long bytes);
  // peak used bytes
  long peak();
  // current used bytes
  long current();
}
```

---

## MemoryUsageStatsBuilder.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/MemoryUsageStatsBuilder.java`

```java
package org.apache.gluten.memory;

public interface MemoryUsageStatsBuilder {
  // Implementation should be idempotent
  MemoryUsageStats toStats();
}
```

---

## SimpleMemoryUsageRecorder.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/SimpleMemoryUsageRecorder.java`

```java
package org.apache.gluten.memory;

// thread safe
public class SimpleMemoryUsageRecorder implements MemoryUsageRecorder {
  private final AtomicLong peak = new AtomicLong(0L);
  private final AtomicLong current = new AtomicLong(0L);

  @Override
  public void inc(long bytes) {
    final long total = this.current.addAndGet(bytes);
    long prevPeak;
    do {
      prevPeak = this.peak.get();
      if (total <= prevPeak) {
        break;
      }
    } while (!this.peak.compareAndSet(prevPeak, total));
  }

  // peak used bytes
  @Override
  public long peak() {
    return peak.get();
  }

  // current used bytes
  @Override
  public long current() {
    return current.get();
  }

  public MemoryUsageStats toStats(Map<String, MemoryUsageStats> children) {
    return MemoryUsageStats.newBuilder()
        .setPeak(peak.get())
        .setCurrent(current.get())
        .putAllChildren(children)
        .build();
  }

  @Override
  public MemoryUsageStats toStats() {
    return toStats(Collections.emptyMap());
  }
}
```

---

## GlobalOffHeapMemory.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemory.scala`

```scala
package org.apache.spark.memory

/**
 * API #acuqire is for reserving some global off-heap memory from Spark memory manager. Once
 * reserved, Spark tasks will have less off-heap memory to use because of the reservation.
 *
 * Note the API #acuqire doesn't trigger spills on Spark tasks although OOM may be encountered.
 *
 * The utility internally relies on the Spark storage memory pool. As Spark doesn't expect trait
 * BlockId to be extended by user, TestBlockId is chosen for the storage memory reservations.
 */
object GlobalOffHeapMemory {
  private val target: MemoryTarget = if (GlutenCoreConfig.get.memoryUntracked) {
    new NoopMemoryTarget()
  } else {
    new GlobalOffHeapMemoryTarget()
  }

  def acquire(numBytes: Long): Unit = {
    if (target.borrow(numBytes) < numBytes) {
      // Throw OOM.
      throw new GlutenException(s"Spark global off-heap memory is exhausted.")
    }
  }

  def release(numBytes: Long): Unit = {
    assert(target.repay(numBytes) == numBytes)
  }

  def currentBytes(): Long = {
    target.usedBytes()
  }
}
```

---

## GlobalOffHeapMemoryTarget.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/memory/GlobalOffHeapMemoryTarget.scala`

```scala
package org.apache.spark.memory

class GlobalOffHeapMemoryTarget private[memory]
  extends MemoryTarget
  with KnownNameAndStats
  with Logging {
  private val targetName = MemoryTargetUtil.toUniqueName("GlobalOffHeap")
  private val recorder: MemoryUsageRecorder = new SimpleMemoryUsageRecorder()
  private val mode: MemoryMode =
    if (GlutenCoreConfig.get.dynamicOffHeapSizingEnabled) MemoryMode.ON_HEAP
    else MemoryMode.OFF_HEAP

  private val FIELD_MEMORY_MANAGER: Field = {
    val f =
      try {
        classOf[TaskMemoryManager].getDeclaredField("memoryManager")
      } catch {
        case e: Exception =>
          throw new GlutenException(
            "Unable to find field TaskMemoryManager#memoryManager via reflection",
            e)
      }
    f.setAccessible(true)
    f
  }

  override def borrow(size: Long): Long = {
    memoryManagerOption()
      .map {
        mm =>
          val succeeded =
            mm.acquireStorageMemory(BlockId(s"test_${UUID.randomUUID()}"), size, mode)

          if (succeeded) {
            recorder.inc(size)
            size
          } else {
            // OOM.
            // Throw OOM.
            if (mode == MemoryMode.ON_HEAP) {
              val storageUsed = mm.onHeapStorageMemoryUsed
              val executionUsed = mm.onHeapExecutionMemoryUsed
              val onHeapMemoryTotal = storageUsed + executionUsed
              logError(
                s"Spark on-heap memory is exhausted. " +
                  s"Requested: $size, " +
                  s"Storage: $storageUsed / $onHeapMemoryTotal, " +
                  s"Execution: $executionUsed / $onHeapMemoryTotal")
              return 0;
            }
            val storageUsed = mm.offHeapStorageMemoryUsed
            val executionUsed = mm.offHeapExecutionMemoryUsed
            val offHeapMemoryTotal = storageUsed + executionUsed
            logError(
              s"Spark off-heap memory is exhausted." +
                s" Storage: $storageUsed / $offHeapMemoryTotal," +
                s" execution: $executionUsed / $offHeapMemoryTotal")
            return 0;
          }
      }
      .getOrElse(size)
  }

  override def repay(size: Long): Long = {
    memoryManagerOption()
      .map {
        mm =>
          mm.releaseStorageMemory(size, mode)
          recorder.inc(-size)
          size
      }
      .getOrElse(size)
  }

  override def usedBytes(): Long = recorder.current()

  override def accept[T](visitor: MemoryTargetVisitor[T]): T = visitor.visit(this)

  private[memory] def memoryManagerOption(): Option[MemoryManager] = {
    val env = SparkEnv.get
    if (env != null) {
      return Some(env.memoryManager)
    }
    val tc = TaskContext.get()
    if (tc != null) {
      // This may happen in test code that mocks the task context without booting up SparkEnv.
      return Some(FIELD_MEMORY_MANAGER.get(tc.taskMemoryManager()).asInstanceOf[MemoryManager])
    }
    logWarning(
      "Memory manager not found because the code is unlikely be run in a Spark application")
    None
  }

  override def name(): String = targetName

  override def stats(): MemoryUsageStats = recorder.toStats
}
```

---

## SparkMemoryUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/memory/SparkMemoryUtil.scala`

```scala
package org.apache.spark.memory

object SparkMemoryUtil {
  private val mmClazz = classOf[MemoryManager]
  private val smpField = mmClazz.getDeclaredField("offHeapStorageMemoryPool")
  private val empField = mmClazz.getDeclaredField("offHeapExecutionMemoryPool")
  smpField.setAccessible(true)
  empField.setAccessible(true)

  private val tmmClazz = classOf[TaskMemoryManager]
  private val consumersField = tmmClazz.getDeclaredField("consumers")
  private val taskIdField = tmmClazz.getDeclaredField("taskAttemptId")
  consumersField.setAccessible(true)
  taskIdField.setAccessible(true)

  def bytesToString(size: Long): String = {
    Utils.bytesToString(size)
  }

  // We assume storage memory can be fully transferred to execution memory so far
  def getCurrentAvailableOffHeapMemory: Long = {
    val mm = SparkEnv.get.memoryManager
    val smp = smpField.get(mm).asInstanceOf[StorageMemoryPool]
    val emp = empField.get(mm).asInstanceOf[ExecutionMemoryPool]
    smp.memoryFree + emp.memoryFree
  }

  def dumpMemoryManagerStats(tmm: TaskMemoryManager): String = {
    val stats = tmm.synchronized {
      val consumers = consumersField.get(tmm).asInstanceOf[util.HashSet[MemoryConsumer]]

      // create stats map
      val statsMap = new util.HashMap[String, MemoryUsageStats]()
      consumers.asScala.foreach {
        case mt: KnownNameAndStats =>
          statsMap.put(mt.name(), mt.stats())
        case mc =>
          statsMap.put(
            mc.toString,
            MemoryUsageStats
              .newBuilder()
              .setCurrent(mc.getUsed)
              .setPeak(-1L)
              .build())
      }
      Preconditions.checkState(statsMap.size() == consumers.size())

      // add root
      new KnownNameAndStats {
        override def name(): String = s"Task.${taskIdField.get(tmm)}"

        override def stats(): MemoryUsageStats = MemoryUsageStats
          .newBuilder()
          .setCurrent(tmm.getMemoryConsumptionForThisTask)
          .setPeak(-1L)
          .putAllChildren(statsMap)
          .build()
      }
    }

    prettyPrintStats("Memory consumer stats: ", stats)
  }

  def dumpMemoryTargetStats(target: MemoryTarget): String = {
    target.accept(new MemoryTargetVisitor[String] {
      override def visit(overAcquire: OverAcquire): String = {
        overAcquire.getTarget.accept(this)
      }

      @nowarn
      override def visit(regularMemoryConsumer: RegularMemoryConsumer): String = {
        collectFromTaskMemoryManager(regularMemoryConsumer.getTaskMemoryManager)
      }

      override def visit(throwOnOomMemoryTarget: ThrowOnOomMemoryTarget): String = {
        throwOnOomMemoryTarget.target().accept(this)
      }

      override def visit(treeMemoryConsumer: TreeMemoryConsumer): String = {
        collectFromTaskMemoryManager(treeMemoryConsumer.getTaskMemoryManager)
      }

      override def visit(node: TreeMemoryConsumer.Node): String = {
        node.parent().accept(this) // walk up to find the one bound with task memory manager
      }

      private def collectFromTaskMemoryManager(tmm: TaskMemoryManager): String = {
        dumpMemoryManagerStats(tmm)
      }

      override def visit(loggingMemoryTarget: LoggingMemoryTarget): String = {
        loggingMemoryTarget.delegated().accept(this)
      }

      override def visit(noopMemoryTarget: NoopMemoryTarget): String = {
        prettyPrintStats("No-op memory target stats: ", noopMemoryTarget)
      }

      override def visit(
          dynamicOffHeapSizingMemoryTarget: DynamicOffHeapSizingMemoryTarget): String = {
        prettyPrintStats(
          "Dynamic off-heap sizing memory target stats: ",
          dynamicOffHeapSizingMemoryTarget)
      }

      override def visit(retryOnOomMemoryTarget: RetryOnOomMemoryTarget): String = {
        retryOnOomMemoryTarget.target().accept(this)
      }

      override def visit(globalOffHeapMemoryTarget: GlobalOffHeapMemoryTarget): String = {
        prettyPrintStats("Global off-heap target stats: ", globalOffHeapMemoryTarget)
      }
    })
  }

  def prettyPrintStats(title: String, stats: KnownNameAndStats): String = {
    def asPrintable(name: String, mus: MemoryUsageStats): PrintableMemoryUsageStats = {
      def sortStats(stats: Seq[PrintableMemoryUsageStats]) = {
        stats.sortBy(_.used.getOrElse(Long.MinValue))(Ordering.Long.reverse)
      }

      PrintableMemoryUsageStats(
        name,
        Some(mus.getCurrent),
        mus.getPeak match {
          case -1L => None
          case v => Some(v)
        },
        sortStats(
          mus.getChildrenMap
            .entrySet()
            .asScala
            .toList
            .map(entry => asPrintable(entry.getKey, entry.getValue)))
      )
    }

    prettyPrintToString(title, asPrintable(stats.name(), stats.stats()))
  }

  private def prettyPrintToString(title: String, stats: PrintableMemoryUsageStats): String = {

    def getBytes(bytes: Option[Long]): String = {
      bytes.map(Utils.bytesToString).getOrElse("N/A")
    }

    def getFullName(name: String, prefix: String): String = {
      "%s%s:".format(prefix, name)
    }

    val sb = new StringBuilder()
    sb.append(title)

    // determine padding widths
    var nameWidth = 0
    var usedWidth = 0
    var peakWidth = 0
    def addPaddingSingleLevel(stats: PrintableMemoryUsageStats, extraWidth: Integer): Unit = {
      nameWidth = Math.max(nameWidth, getFullName(stats.name, "").length + extraWidth)
      usedWidth = Math.max(usedWidth, getBytes(stats.used).length)
      peakWidth = Math.max(peakWidth, getBytes(stats.peak).length)
      stats.children.foreach(addPaddingSingleLevel(_, extraWidth + 3)) // e.g. "\- "
    }
    addPaddingSingleLevel(stats, 1) // take the leading '\t' into account

    // print
    def printSingleLevel(
        stats: PrintableMemoryUsageStats,
        treePrefix: String,
        treeChildrenPrefix: String): Unit = {
      sb.append(System.lineSeparator())

      val name = getFullName(stats.name, treePrefix)
      sb.append(
        s"%s Current used bytes: %s, peak bytes: %s"
          .format(
            StringUtils.rightPad(name, nameWidth, ' '),
            StringUtils.leftPad(String.valueOf(getBytes(stats.used)), usedWidth, ' '),
            StringUtils.leftPad(String.valueOf(getBytes(stats.peak)), peakWidth, ' ')
          ))

      stats.children.zipWithIndex.foreach {
        case (child, i) =>
          if (i != stats.children.size - 1) {
            printSingleLevel(child, treeChildrenPrefix + "+- ", treeChildrenPrefix + "|  ")
          } else {
            printSingleLevel(child, treeChildrenPrefix + "\\- ", treeChildrenPrefix + "   ")
          }
      }
    }

    printSingleLevel(
      stats,
      "\t",
      "\t"
    ) // top level is indented with one tab (align with exception stack trace)

    // return
    sb.toString()
  }

  private case class PrintableMemoryUsageStats(
      name: String,
      used: Option[Long],
      peak: Option[Long],
      children: Iterable[PrintableMemoryUsageStats])
}
```

---

# memtarget

*This section contains 15 source files.*

## DynamicOffHeapSizingMemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/DynamicOffHeapSizingMemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

/**
 * The memory target used by dynamic off-heap sizing. Since
 * https://github.com/apache/incubator-gluten/issues/5439.
 */
@Experimental
public class DynamicOffHeapSizingMemoryTarget implements MemoryTarget, KnownNameAndStats {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicOffHeapSizingMemoryTarget.class);
  // When dynamic off-heap sizing is enabled, the off-heap should be sized for the total usable
  // memory, so we can use it as the max memory we will use.
  private static final long TOTAL_MEMORY_SHARED;

  private static final double ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO = 0.85;
  private static final double ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO = 0.65;
  private static final double GC_MAX_HEAP_FREE_RATIO = 0.05;
  private static final int MAX_GC_RETRY_TIMES = 3;

  private static final AtomicBoolean ASYNC_GC_SUSPEND = new AtomicBoolean(false);
  private static final Object JVM_SHRINK_SYNC_OBJECT = new Object();

  private static final int ORIGINAL_MAX_HEAP_FREE_RATIO;
  private static final int ORIGINAL_MIN_HEAP_FREE_RATIO;

  static {
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    List<String> jvmArgs = runtimeMxBean.getInputArguments();
    int originalMaxHeapFreeRatio = 70;
    int originalMinHeapFreeRatio = 40;
    for (String arg : jvmArgs) {
      if (arg.startsWith("-XX:MaxHeapFreeRatio=")) {
        String valuePart = arg.substring(arg.indexOf('=') + 1);
        try {
          originalMaxHeapFreeRatio = Integer.parseInt(valuePart);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse MaxHeapFreeRatio from JVM argument: {}. Using default value: {}.",
              arg,
              originalMaxHeapFreeRatio);
        }
      } else if (arg.startsWith("-XX:MinHeapFreeRatio=")) {
        String valuePart = arg.substring(arg.indexOf('=') + 1);
        try {
          originalMinHeapFreeRatio = Integer.parseInt(valuePart);
        } catch (NumberFormatException e) {
          LOG.warn(
              "Failed to parse MinHeapFreeRatio from JVM argument: {}. Using default value: {}.",
              arg,
              originalMinHeapFreeRatio);
        }
      } else if (Objects.equals(arg, "-XX:+ExplicitGCInvokesConcurrent")) {
        // If this is set -XX:+ExplicitGCInvokesConcurrent, System.gc() does not trigger Full GC,
        // so explicit JVM shrinking is not effective.
        LOG.error(
            "Explicit JVM shrinking is not effective because -XX:+ExplicitGCInvokesConcurrent"
                + " is set. Please check the JVM arguments: {}. ",
            arg);

      } else if (Objects.equals(arg, "-XX:+DisableExplicitGC")) {
        // If -XX:+DisableExplicitGC is set, calls to System.gc() are ignored,
        // so explicit JVM shrinking will not work as intended.
        LOG.error(
            "Explicit JVM shrinking is disabled because -XX:+DisableExplicitGC is set. "
                + "System.gc() calls will be ignored and JVM shrinking will not work. "
                + "Please check the JVM arguments: {}. ",
            arg);
      }
    }
    ORIGINAL_MIN_HEAP_FREE_RATIO = originalMinHeapFreeRatio;
    ORIGINAL_MAX_HEAP_FREE_RATIO = originalMaxHeapFreeRatio;

    if (!isJava9OrLater()) {
      // For JDK 8, we cannot change MaxHeapFreeRatio programmatically at runtime.
      LOG.error("Dynamic off-heap sizing is not supported before JDK 9.");
    }

    TOTAL_MEMORY_SHARED = Runtime.getRuntime().maxMemory();

    LOG.info(
        "Initialized DynamicOffHeapSizingMemoryTarget with MAX_MEMORY_IN_BYTES = {}, "
            + "ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO = {}, "
            + "ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO = {}, "
            + "GC_MAX_HEAP_FREE_RATIO = {}.",
        TOTAL_MEMORY_SHARED,
        ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO,
        ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO,
        GC_MAX_HEAP_FREE_RATIO);
  }

  private static final AtomicLong USED_OFF_HEAP_BYTES = new AtomicLong();

  // Test only.
  private static final AtomicLong TOTAL_EXPLICIT_GC_COUNT = new AtomicLong(0L);

  private final String name = MemoryTargetUtil.toUniqueName("DynamicOffHeapSizing");
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();

  public DynamicOffHeapSizingMemoryTarget() {}

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }

    // Only JVM shrinking can reclaim space from the total JVM memory.
    // See https://github.com/apache/incubator-gluten/issues/9276.
    long totalHeapMemory = Runtime.getRuntime().totalMemory();
    long freeHeapMemory = Runtime.getRuntime().freeMemory();
    long usedOffHeapMemory = USED_OFF_HEAP_BYTES.get();

    // Adds the total JVM memory which is the actual memory the JVM occupied from the operating
    // system into the counter.
    if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
      // Perform GC synchronously to shrink memory; native tasks need to wait for this to obtain
      // more memory.
      synchronized (JVM_SHRINK_SYNC_OBJECT) {
        totalHeapMemory = Runtime.getRuntime().totalMemory();
        freeHeapMemory = Runtime.getRuntime().freeMemory();
        if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
          shrinkOnHeapMemory(totalHeapMemory, freeHeapMemory, false);
          totalHeapMemory = Runtime.getRuntime().totalMemory();
          freeHeapMemory = Runtime.getRuntime().freeMemory();
        }
      }
      // Check if we can allocate the requested size again after JVM shrinking(GC).
      if (exceedsMaxMemoryUsage(totalHeapMemory, usedOffHeapMemory, size, 1.0)) {
        LOG.warn(
            String.format(
                "Failing allocation as unified memory is OOM. "
                    + "Used Off-heap: %d, Used On-Heap: %d, "
                    + "Free On-heap: %d, Total On-heap: %d, "
                    + "Max On-heap: %d, Allocation: %d.",
                usedOffHeapMemory,
                totalHeapMemory - freeHeapMemory,
                freeHeapMemory,
                totalHeapMemory,
                TOTAL_MEMORY_SHARED,
                size));

        return 0;
      }
    } else if (shouldTriggerAsyncOnHeapMemoryShrink(
        totalHeapMemory, freeHeapMemory, usedOffHeapMemory, size)) {
      // Proactively trigger memory shrinking in the thread pool to prevent GC from blocking
      // native task execution.
      SparkThreadPoolUtil.triggerGCInThreadPool(
          new Runnable() {
            @Override
            public void run() {
              synchronized (JVM_SHRINK_SYNC_OBJECT) {
                long totalJvmMem = Runtime.getRuntime().totalMemory();
                long freeJvmMem = Runtime.getRuntime().freeMemory();
                if (shouldTriggerAsyncOnHeapMemoryShrink(
                    totalJvmMem, freeJvmMem, usedOffHeapMemory, size)) {
                  shrinkOnHeapMemory(totalJvmMem, freeJvmMem, true);
                }
              }
            }
          });
    }

    USED_OFF_HEAP_BYTES.addAndGet(size);
    recorder.inc(size);
    return size;
  }

  @Override
  public long repay(long size) {
    USED_OFF_HEAP_BYTES.addAndGet(-size);
    recorder.inc(-size);
    return size;
  }

  @Override
  public long usedBytes() {
    return recorder.current();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MemoryUsageStats stats() {
    return recorder.toStats();
  }

  public static boolean isJava9OrLater() {
    String spec = System.getProperty("java.specification.version", "1.8");
    // "1.8" → 8, "9" → 9, "11" → 11, etc.
    if (spec.startsWith("1.")) {
      spec = spec.substring(2);
    }
    try {
      return Integer.parseInt(spec) >= 9;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean canShrinkJVMMemory(long totalMemory, long freeMemory) {
    // Check if the JVM memory can be shrunk by a full GC.
    return freeMemory > totalMemory * GC_MAX_HEAP_FREE_RATIO;
  }

  public static long getTotalExplicitGCCount() {
    return TOTAL_EXPLICIT_GC_COUNT.get();
  }

  private static boolean exceedsMaxMemoryUsage(
      long totalOnHeapMemory, long totalOffHeapMemory, long requestedSize, double ratio) {
    return requestedSize + totalOffHeapMemory + totalOnHeapMemory >= TOTAL_MEMORY_SHARED * ratio;
  }

  private static boolean shouldTriggerAsyncOnHeapMemoryShrink(
      long totalOnHeapMemory, long freeOnHeapMemory, long totalOffHeapMemory, long requestedSize) {
    // If most of the memory has already been used, there's a high chance that memory will be fully
    // consumed. We proactively detect this situation to trigger JVM memory shrinking using the
    // following conditions.

    boolean exceedsMaxMemoryUsageRatio =
        exceedsMaxMemoryUsage(
            totalOnHeapMemory,
            totalOffHeapMemory,
            requestedSize,
            ASYNC_GC_MAX_TOTAL_MEMORY_USAGE_RATIO);
    return exceedsMaxMemoryUsageRatio
        && canShrinkJVMMemory(totalOnHeapMemory, freeOnHeapMemory)
        // Limit GC frequency to prevent performance impact from excessive garbage collection.
        && totalOnHeapMemory > TOTAL_MEMORY_SHARED * ASYNC_GC_MAX_ON_HEAP_MEMORY_RATIO
        && (!ASYNC_GC_SUSPEND.get()
            || freeOnHeapMemory > totalOnHeapMemory * (ORIGINAL_MIN_HEAP_FREE_RATIO / 100.0));
  }

  private static long shrinkOnHeapMemoryInternal(
      long totalMemory, long freeMemory, boolean isAsyncGc) {
    long totalOffHeapMemory = USED_OFF_HEAP_BYTES.get();
    LOG.warn(
        String.format(
            "Starting %sfull gc to shrink JVM memory: "
                + "Total On-heap: %d, Free On-heap: %d, "
                + "Total Off-heap: %d, Used On-Heap: %d, Executor memory: %d.",
            isAsyncGc ? "async " : "",
            totalMemory,
            freeMemory,
            totalOffHeapMemory,
            (totalMemory - freeMemory),
            TOTAL_MEMORY_SHARED));
    // Explicitly calling System.gc() to trigger a full garbage collection.
    // This is necessary in this context to attempt to shrink JVM memory usage
    // when off-heap memory allocation is constrained. Use of System.gc() is
    // generally discouraged due to its unpredictable performance impact, but
    // here it is used as a last resort to prevent memory allocation failures.
    System.gc();
    long newTotalMemory = Runtime.getRuntime().totalMemory();
    long newFreeMemory = Runtime.getRuntime().freeMemory();
    int gcRetryTimes = 0;
    while (!isAsyncGc
        && gcRetryTimes < MAX_GC_RETRY_TIMES
        && newTotalMemory >= totalMemory
        && canShrinkJVMMemory(newTotalMemory, newFreeMemory)) {
      // System.gc() is just a suggestion; the JVM may ignore it or perform only a partial GC.
      // Here, the total memory is not reduced but the free memory ratio is bigger than the
      // GC_MAX_HEAP_FREE_RATIO. So we need to call System.gc() again to try to reduce the total
      // memory.
      // This is a workaround for the JVM's behavior of not reducing the total memory after GC.
      System.gc();
      newTotalMemory = Runtime.getRuntime().totalMemory();
      newFreeMemory = Runtime.getRuntime().freeMemory();
      gcRetryTimes++;
    }
    // If the memory usage is still high after GC, we need to suspend the async GC for a while.
    if (isAsyncGc) {
      ASYNC_GC_SUSPEND.set(
          totalMemory - newTotalMemory < totalMemory * (ORIGINAL_MIN_HEAP_FREE_RATIO / 100.0));
    }

    TOTAL_EXPLICIT_GC_COUNT.getAndAdd(1);
    LOG.warn(
        String.format(
            "Finished %sfull gc to shrink JVM memory: "
                + "Total On-heap: %d, Free On-heap: %d, "
                + "Total Off-heap: %d, Used On-Heap: %d, Executor memory: %d, "
                + "[GC Retry times: %d].",
            isAsyncGc ? "async " : "",
            newTotalMemory,
            newFreeMemory,
            totalOffHeapMemory,
            (newTotalMemory - newFreeMemory),
            TOTAL_MEMORY_SHARED,
            gcRetryTimes));
    return newTotalMemory;
  }

  public static long shrinkOnHeapMemory(long totalMemory, long freeMemory, boolean isAsyncGc) {
    boolean updateMaxHeapFreeRatio = false;
    Object hotSpotBean = null;
    String maxHeapFreeRatioName = "MaxHeapFreeRatio";
    String minHeapFreeRatioName = "MinHeapFreeRatio";
    int newValue = (int) (GC_MAX_HEAP_FREE_RATIO * 100);

    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      Class<?> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
      hotSpotBean =
          ManagementFactory.newPlatformMXBeanProxy(
              mbs, "com.sun.management:type=HotSpotDiagnostic", beanClass);

      Method setOption = beanClass.getMethod("setVMOption", String.class, String.class);
      if (newValue < ORIGINAL_MIN_HEAP_FREE_RATIO) {
        // Adjust the MinHeapFreeRatio to avoid the violation of the MaxHeapFreeRatio.
        setOption.invoke(hotSpotBean, minHeapFreeRatioName, Integer.toString(newValue));
      }
      if (newValue < ORIGINAL_MAX_HEAP_FREE_RATIO) {
        setOption.invoke(hotSpotBean, maxHeapFreeRatioName, Integer.toString(newValue));
        updateMaxHeapFreeRatio = true;
        LOG.info(
            String.format(
                "Updated VM flags: MaxHeapFreeRatio from %d to %d.",
                ORIGINAL_MAX_HEAP_FREE_RATIO, newValue));
      }
      return shrinkOnHeapMemoryInternal(totalMemory, freeMemory, isAsyncGc);
    } catch (Exception e) {
      LOG.warn(
          "Failed to update JVM heap free ratio via HotSpotDiagnosticMXBean: {}", e.toString());
      return totalMemory;
    } finally {
      // Reset the MaxHeapFreeRatio to the original values.
      if (hotSpotBean != null && updateMaxHeapFreeRatio) {
        try {
          Class<?> beanClass = Class.forName("com.sun.management.HotSpotDiagnosticMXBean");
          Method setOption = beanClass.getMethod("setVMOption", String.class, String.class);
          setOption.invoke(
              hotSpotBean, maxHeapFreeRatioName, Integer.toString(ORIGINAL_MAX_HEAP_FREE_RATIO));
          LOG.info("Reverted VM flags back.");
        } catch (Exception ignore) {
          // best‐effort revert
        }
      }
    }
  }
}
```

---

## KnownNameAndStats.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/KnownNameAndStats.java`

```java
package org.apache.gluten.memory.memtarget;

/**
 * The implementation provides a String name and {@link MemoryUsageStats}.
 *
 * <p>The API is used to format the dumped message when utility method {@link
 * org.apache.spark.memory.SparkMemoryUtil#dumpMemoryTargetStats} is called.
 */
public interface KnownNameAndStats {
  String name();

  MemoryUsageStats stats();
}
```

---

## LoggingMemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/LoggingMemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

// For debugging purpose only
public class LoggingMemoryTarget implements MemoryTarget {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMemoryTarget.class);
  private final MemoryTarget delegated;

  public LoggingMemoryTarget(MemoryTarget delegated) {
    this.delegated = delegated;
  }

  @Override
  public long borrow(long size) {
    long before = usedBytes();
    long reserved = delegated.borrow(size);
    long after = usedBytes();
    LOGGER.info(
        String.format(
            "Borrowed[%s]: %d + %d(%d) = %d", this.toString(), before, reserved, size, after));
    return reserved;
  }

  @Override
  public long repay(long size) {
    long before = usedBytes();
    long unreserved = delegated.repay(size);
    long after = usedBytes();
    LOGGER.info(
        String.format(
            "Repaid[%s]: %d - %d(%d) = %d", this.toString(), before, unreserved, size, after));
    return unreserved;
  }

  @Override
  public long usedBytes() {
    return delegated.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public MemoryTarget delegated() {
    return delegated;
  }
}
```

---

## MemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

// The naming convention "borrow" and "repay" are for preventing collisions with
//   other APIs.
//
// Implementations are not necessary to be thread-safe
public interface MemoryTarget {
  long borrow(long size);

  long repay(long size);

  long usedBytes();

  <T> T accept(MemoryTargetVisitor<T> visitor);
}
```

---

## MemoryTargetUtil.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTargetUtil.java`

```java
package org.apache.gluten.memory.memtarget;

public final class MemoryTargetUtil {
  private MemoryTargetUtil() {}

  private static final Map<String, Integer> UNIQUE_NAME_LOOKUP = new ConcurrentHashMap<>();

  public static String toUniqueName(String name) {
    int nextId =
        UNIQUE_NAME_LOOKUP.compute(
            name, (s, integer) -> Optional.ofNullable(integer).map(id -> id + 1).orElse(0));
    return String.format("%s.%d", name, nextId);
  }
}
```

---

## MemoryTargetVisitor.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTargetVisitor.java`

```java
package org.apache.gluten.memory.memtarget;

public interface MemoryTargetVisitor<T> {
  T visit(OverAcquire overAcquire);

  T visit(RegularMemoryConsumer regularMemoryConsumer);

  T visit(ThrowOnOomMemoryTarget throwOnOomMemoryTarget);

  T visit(TreeMemoryConsumer treeMemoryConsumer);

  T visit(TreeMemoryConsumer.Node node);

  T visit(LoggingMemoryTarget loggingMemoryTarget);

  T visit(NoopMemoryTarget noopMemoryTarget);

  T visit(DynamicOffHeapSizingMemoryTarget dynamicOffHeapSizingMemoryTarget);

  T visit(RetryOnOomMemoryTarget retryOnOomMemoryTarget);

  T visit(GlobalOffHeapMemoryTarget globalOffHeapMemoryTarget);
}
```

---

## MemoryTargets.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/MemoryTargets.java`

```java
package org.apache.gluten.memory.memtarget;

public final class MemoryTargets {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemoryTargets.class);

  private MemoryTargets() {
    // enclose factory ctor
  }

  public static MemoryTarget throwOnOom(MemoryTarget target) {
    return new ThrowOnOomMemoryTarget(target);
  }

  public static MemoryTarget overAcquire(
      MemoryTarget target, MemoryTarget overTarget, double overAcquiredRatio) {
    if (overAcquiredRatio == 0.0D) {
      return target;
    }
    return new OverAcquire(target, overTarget, overAcquiredRatio);
  }

  @Experimental
  public static MemoryTarget dynamicOffHeapSizingIfEnabled(MemoryTarget memoryTarget) {
    if (GlutenCoreConfig.get().dynamicOffHeapSizingEnabled()) {
      return new DynamicOffHeapSizingMemoryTarget();
    }

    return memoryTarget;
  }

  public static TreeMemoryTarget newConsumer(
      TaskMemoryManager tmm,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    final MemoryMode mode;
    if (GlutenCoreConfig.get().dynamicOffHeapSizingEnabled()) {
      mode = MemoryMode.ON_HEAP;
    } else {
      mode = MemoryMode.OFF_HEAP;
    }
    final TreeMemoryConsumers.Factory factory = TreeMemoryConsumers.factory(tmm, mode);
    if (GlutenCoreConfig.get().memoryIsolation()) {
      return TreeMemoryTargets.newChild(factory.isolatedRoot(), name, spiller, virtualChildren);
    }
    final TreeMemoryTarget root = factory.legacyRoot();
    final TreeMemoryTarget consumer =
        TreeMemoryTargets.newChild(root, name, spiller, virtualChildren);
    if (SparkEnv.get() == null) {
      // We are likely in test code. Return the consumer directly.
      LOGGER.info("SparkEnv not found. We are likely in test code.");
      return consumer;
    }
    final int taskSlots = SparkResourceUtil.getTaskSlots(SparkEnv.get().conf());
    if (taskSlots == 1) {
      // We don't need to retry on OOM in the case one single task occupies the whole executor.
      return consumer;
    }
    // Since https://github.com/apache/incubator-gluten/pull/8132.
    // Retry of spilling is needed in multi-slot and legacy mode (formerly named as share mode)
    // because the maxMemoryPerTask defined by vanilla Spark's ExecutionMemoryPool is dynamic.
    //
    // See the original issue https://github.com/apache/incubator-gluten/issues/8128.
    return new RetryOnOomMemoryTarget(
        consumer,
        () -> {
          LOGGER.info("Request for spilling on consumer {}...", consumer.name());
          // Note: Spill from root node so other consumers also get spilled.
          long spilled = TreeMemoryTargets.spillTree(root, Long.MAX_VALUE);
          LOGGER.info("Consumer {} gets {} bytes from spilling.", consumer.name(), spilled);
        });
  }
}
```

---

## NoopMemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/NoopMemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

public class NoopMemoryTarget implements MemoryTarget, KnownNameAndStats {
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final String name = MemoryTargetUtil.toUniqueName("Noop");

  @Override
  public long borrow(long size) {
    recorder.inc(size);
    return size;
  }

  @Override
  public long repay(long size) {
    recorder.inc(-size);
    return size;
  }

  @Override
  public long usedBytes() {
    return recorder.current();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MemoryUsageStats stats() {
    return recorder.toStats();
  }
}
```

---

## OverAcquire.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/OverAcquire.java`

```java
package org.apache.gluten.memory.memtarget;

public class OverAcquire implements MemoryTarget {

  // The underlying target.
  private final MemoryTarget target;

  // This consumer holds the over-acquired memory.
  private final MemoryTarget overTarget;

  // The ratio is normally 0.
  //
  // If set to some value other than 0, the consumer will try
  //   over-acquire this ratio of memory each time it acquires
  //   from Spark.
  //
  // Once OOM, the over-acquired memory will be used as backup.
  //
  // The over-acquire is a general workaround for underling reservation
  //   procedures that were not perfectly-designed for spilling. For example,
  //   reservation for a two-step procedure: step A is capable for
  //   spilling while step B is not. If not reserving enough memory
  //   for step B before it's started, it might raise OOM since step A
  //   is ended and no longer open for spilling. In this case the
  //   over-acquired memory will be used in step B.
  private final double ratio;

  OverAcquire(MemoryTarget target, MemoryTarget overTarget, double ratio) {
    Preconditions.checkArgument(ratio >= 0.0D);
    this.overTarget = overTarget;
    this.target = target;
    this.ratio = ratio;
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }
    Preconditions.checkState(overTarget.usedBytes() == 0);
    long granted = target.borrow(size);
    if (granted >= size) {
      long majorSize = target.usedBytes();
      long overSize = (long) (ratio * majorSize);
      long overAcquired = overTarget.borrow(overSize);
      Preconditions.checkState(overAcquired == overTarget.usedBytes());
      long releasedOverSize = overTarget.repay(overAcquired);
      Preconditions.checkState(releasedOverSize == overAcquired);
      Preconditions.checkState(overTarget.usedBytes() == 0);
    }
    return granted;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes() + overTarget.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public MemoryTarget getTarget() {
    return target;
  }
}
```

---

## RetryOnOomMemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/RetryOnOomMemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

public class RetryOnOomMemoryTarget implements TreeMemoryTarget {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetryOnOomMemoryTarget.class);
  private final TreeMemoryTarget target;
  private final Runnable onRetry;

  RetryOnOomMemoryTarget(TreeMemoryTarget target, Runnable onRetry) {
    this.target = target;
    this.onRetry = onRetry;
  }

  @Override
  public long borrow(long size) {
    long granted = target.borrow(size);
    if (granted < size) {
      LOGGER.info("Granted size {} is less than requested size {}, retrying...", granted, size);
      final long remaining = size - granted;
      // Invoke the `onRetry` callback, then retry borrowing.
      // It's usually expected to run extra spilling logics in
      // the `onRetry` callback so we may get enough memory space
      // to allocate the remaining bytes.
      onRetry.run();
      granted += target.borrow(remaining);
      LOGGER.info("Newest granted size after retrying: {}, requested size {}.", granted, size);
    }
    return granted;
  }

  @Override
  public long repay(long size) {
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return target.name();
  }

  @Override
  public MemoryUsageStats stats() {
    return target.stats();
  }

  @Override
  public TreeMemoryTarget newChild(
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    return target.newChild(name, capacity, spiller, virtualChildren);
  }

  @Override
  public Map<String, TreeMemoryTarget> children() {
    return target.children();
  }

  @Override
  public TreeMemoryTarget parent() {
    return target.parent();
  }

  @Override
  public Spiller getNodeSpiller() {
    return target.getNodeSpiller();
  }

  public TreeMemoryTarget target() {
    return target;
  }
}
```

---

## Spiller.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spiller.java`

```java
package org.apache.gluten.memory.memtarget;

public interface Spiller {
  long spill(MemoryTarget self, Phase phase, long size);

  // Order of the elements matters, since
  // consumer should call spillers with in the defined order.
  enum Phase {
    SHRINK,
    SPILL
  }
}
```

---

## Spillers.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/Spillers.java`

```java
package org.apache.gluten.memory.memtarget;

public final class Spillers {
  private Spillers() {
    // enclose factory ctor
  }

  public static final Spiller NOOP =
      new Spiller() {
        @Override
        public long spill(MemoryTarget self, Phase phase, long size) {
          return 0;
        }
      };

  public static final Set<Spiller.Phase> PHASE_SET_ALL =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList(Spiller.Phase.SHRINK, Spiller.Phase.SPILL)));

  public static Spiller withMinSpillSize(Spiller spiller, long minSize) {
    return new WithMinSpillSize(spiller, minSize);
  }

  public static AppendableSpillerList appendable() {
    return new AppendableSpillerList();
  }

  // Minimum spill target size should be larger than spark.gluten.memory.reservationBlockSize,
  // since any release action within size smaller than the block size may not have chance to
  // report back to the Java-side reservation listener.
  private static class WithMinSpillSize implements Spiller {
    private final Spiller delegated;
    private final long minSize;

    private WithMinSpillSize(Spiller delegated, long minSize) {
      this.delegated = delegated;
      this.minSize = minSize;
    }

    @Override
    public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
      return delegated.spill(self, phase, Math.max(size, minSize));
    }
  }

  public static class AppendableSpillerList implements Spiller {
    private final List<Spiller> spillers = new LinkedList<>();

    private AppendableSpillerList() {}

    public void append(Spiller spiller) {
      spillers.add(spiller);
    }

    @Override
    public long spill(MemoryTarget self, Phase phase, final long size) {
      long remainingBytes = size;
      for (Spiller spiller : spillers) {
        if (remainingBytes <= 0) {
          break;
        }
        remainingBytes -= spiller.spill(self, phase, remainingBytes);
      }
      return size - remainingBytes;
    }
  }
}
```

---

## ThrowOnOomMemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/ThrowOnOomMemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

public class ThrowOnOomMemoryTarget implements MemoryTarget {
  private final MemoryTarget target;

  public ThrowOnOomMemoryTarget(MemoryTarget target) {
    this.target = target;
  }

  @Override
  public long borrow(long size) {
    long granted = target.borrow(size);
    if (granted >= size) {
      return granted;
    }
    // OOM happens.
    // Note if the target is a Spark memory consumer, spilling should already be requested but
    // failed to reclaim enough memory.
    if (granted != 0L) {
      target.repay(granted);
    }
    // Log memory usage
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMemoryManager().showMemoryUsage();
    }
    // Build error message, then throw
    StringBuilder errorBuilder = new StringBuilder();
    errorBuilder
        .append(
            String.format(
                "Not enough spark off-heap execution memory. Acquired: %s, granted: %s. "
                    + "Try tweaking config option spark.memory.offHeap.size to get larger "
                    + "space to run this application "
                    + "(if spark.gluten.memory.dynamic.offHeap.sizing.enabled "
                    + "is not enabled). %n",
                Utils.bytesToString(size), Utils.bytesToString(granted)))
        .append("Current config settings: ")
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES().key(),
                reformatBytes(
                    SQLConf.get()
                        .getConfString(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES().key()))))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES().key(),
                reformatBytes(
                    SQLConf.get()
                        .getConfString(
                            GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES().key()))))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(),
                reformatBytes(
                    SQLConf.get()
                        .getConfString(
                            GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES()
                                .key()))))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY(),
                SQLConf.get().getConfString(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY())))
        .append(System.lineSeparator())
        .append(
            String.format(
                "\t%s=%s",
                GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED().key(),
                GlutenCoreConfig.get().dynamicOffHeapSizingEnabled()))
        .append(System.lineSeparator());
    // Dump all consumer usages to exception body
    errorBuilder.append(SparkMemoryUtil.dumpMemoryTargetStats(target));
    errorBuilder.append(System.lineSeparator());
    throw new OutOfMemoryException(errorBuilder.toString());
  }

  private static String reformatBytes(String in) {
    return Utils.bytesToString(Utils.byteStringAsBytes(in));
  }

  @Override
  public long repay(long size) {
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public static class OutOfMemoryException extends RuntimeException {
    public OutOfMemoryException(String message) {
      super(message);
    }
  }

  public MemoryTarget target() {
    return target;
  }
}
```

---

## TreeMemoryTarget.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTarget.java`

```java
package org.apache.gluten.memory.memtarget;

/** An abstract for both {@link TreeMemoryConsumer} and it's non-consumer children nodes. */
public interface TreeMemoryTarget extends MemoryTarget, KnownNameAndStats {
  long CAPACITY_UNLIMITED = Long.MAX_VALUE;

  TreeMemoryTarget newChild(
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren);

  Map<String, TreeMemoryTarget> children();

  TreeMemoryTarget parent();

  Spiller getNodeSpiller();
}
```

---

## TreeMemoryTargets.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/TreeMemoryTargets.java`

```java
package org.apache.gluten.memory.memtarget;

public class TreeMemoryTargets {

  private TreeMemoryTargets() {
    // enclose factory ctor
  }

  /**
   * A short-cut method to create a child target of `parent`. The child will follow the parent's
   * maximum capacity.
   */
  static TreeMemoryTarget newChild(
      TreeMemoryTarget parent,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    return parent.newChild(name, TreeMemoryTarget.CAPACITY_UNLIMITED, spiller, virtualChildren);
  }

  public static long spillTree(TreeMemoryTarget node, final long bytes) {
    long remainingBytes = bytes;
    for (Spiller.Phase phase : Spiller.Phase.values()) {
      // First shrink, then if no good, spill.
      if (remainingBytes <= 0) {
        break;
      }
      remainingBytes -= spillTree(node, phase, remainingBytes);
    }
    return bytes - remainingBytes;
  }

  private static long spillTree(TreeMemoryTarget node, Spiller.Phase phase, final long bytes) {
    // sort children by used bytes, descending
    Queue<TreeMemoryTarget> q =
        new PriorityQueue<>(
            (o1, o2) -> {
              long diff = o1.usedBytes() - o2.usedBytes();
              return -(diff > 0 ? 1 : diff < 0 ? -1 : 0); // descending
            });
    q.addAll(node.children().values());

    long remainingBytes = bytes;
    while (q.peek() != null && remainingBytes > 0) {
      TreeMemoryTarget head = q.remove();
      long spilled = spillTree(head, phase, remainingBytes);
      remainingBytes -= spilled;
    }

    if (remainingBytes > 0) {
      // if still doesn't fit, spill self
      final Spiller spiller = node.getNodeSpiller();
      long spilled = spiller.spill(node, phase, remainingBytes);
      remainingBytes -= spilled;
    }

    return bytes - remainingBytes;
  }
}
```

---

# metadata

*This section contains 4 source files.*

## GlutenMetadata.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/metadata/GlutenMetadata.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.metadata

sealed trait GlutenMetadata extends Metadata {
  def schema(): Schema
  def logicalLink(): LogicalLink
}

object GlutenMetadata {
  def apply(schema: Schema, logicalLink: LogicalLink): Metadata = {
    Impl(schema, logicalLink)
  }

  private case class Impl(override val schema: Schema, override val logicalLink: LogicalLink)
    extends GlutenMetadata {
    override def toString: String = s"$schema,$logicalLink"
  }
}
```

---

## GlutenMetadataModel.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/metadata/GlutenMetadataModel.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.metadata

object GlutenMetadataModel extends Logging {
  def apply(): MetadataModel[SparkPlan] = {
    MetadataModelImpl
  }

  private object MetadataModelImpl extends MetadataModel[SparkPlan] {
    override def metadataOf(node: SparkPlan): Metadata = node match {
      case g: GroupLeafExec => throw new UnsupportedOperationException()
      case other =>
        GlutenMetadata(
          Schema(other.output),
          other.logicalLink.map(LogicalLink(_)).getOrElse(LogicalLink.notFound))
    }

    override def dummy(): Metadata = GlutenMetadata(Schema(List()), LogicalLink.notFound)
    override def verify(one: Metadata, other: Metadata): Unit = (one, other) match {
      case (left: GlutenMetadata, right: GlutenMetadata) =>
        implicitly[Verifier[Schema]].verify(left.schema(), right.schema())
        implicitly[Verifier[LogicalLink]].verify(left.logicalLink(), right.logicalLink())
      case _ => throw new IllegalStateException(s"Metadata mismatch: one: $one, other $other")
    }

    override def assignToGroup(group: GroupLeafBuilder[SparkPlan], meta: Metadata): Unit =
      (group, meta) match {
        case (builder: GroupLeafExec.Builder, metadata: GlutenMetadata) =>
          builder.withMetadata(metadata)
      }
  }

  trait Verifier[T <: Any] {
    def verify(one: T, other: T): Unit
  }
}
```

---

## LogicalLink.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/metadata/LogicalLink.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.metadata

case class LogicalLink(plan: LogicalPlan) {
  override def hashCode(): Int = System.identityHashCode(plan)
  override def equals(obj: Any): Boolean = obj match {
    // LogicalLink's comparison is based on ref equality of the logical plans being compared.
    case LogicalLink(otherPlan) => plan eq otherPlan
    case _ => false
  }
  override def toString: String = s"${plan.nodeName}[${plan.stats.simpleString}]"
}

object LogicalLink {
  private case class LogicalLinkNotFound() extends logical.LeafNode {
    override def output: Seq[Attribute] = List.empty
    override def canEqual(that: Any): Boolean = throw new UnsupportedOperationException()
    override def computeStats(): Statistics = Statistics(sizeInBytes = 0)
  }

  val notFound = new LogicalLink(LogicalLinkNotFound())
  implicit val verifier: Verifier[LogicalLink] = new Verifier[LogicalLink] with Logging {
    override def verify(one: LogicalLink, other: LogicalLink): Unit = {
      // LogicalLink's comparison is based on ref equality of the logical plans being compared.
      if (one != other) {
        logWarning(s"Warning: Logical link mismatch: one: $one, other: $other")
      }
    }
  }
}
```

---

## Schema.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/metadata/Schema.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.metadata

case class Schema(output: Seq[Attribute]) {
  private val hash = output.map(_.semanticHash()).hashCode()

  override def hashCode(): Int = {
    hash
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: Schema =>
      semanticEquals(other)
    case _ =>
      false
  }

  private def semanticEquals(other: Schema): Boolean = {
    if (output.size != other.output.size) {
      return false
    }
    output.zip(other.output).forall {
      case (left, right) =>
        left.semanticEquals(right)
    }
  }

  override def toString: String = {
    output.toString()
  }
}

object Schema {
  implicit val verifier: Verifier[Schema] = new Verifier[Schema] with Logging {
    override def verify(one: Schema, other: Schema): Unit = {
      if (one != other) {
        // We apply loose restriction on schema. Since Gluten still have some customized
        // logics causing schema of an operator to change after being transformed.
        // For example: https://github.com/apache/incubator-gluten/pull/5171
        logWarning(s"Warning: Schema mismatch: one: $one, other: $other")
      }
    }
  }
}
```

---

# offload

*This section contains 1 source files.*

## OffloadSingleNode.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/offload/OffloadSingleNode.scala`

```scala
package org.apache.gluten.extension.columnar.offload

/**
 * Converts a vanilla Spark plan node into Gluten plan node. Gluten plan is supposed to be executed
 * in native, and the internals of execution is subject by backend's implementation.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 */
trait OffloadSingleNode extends Logging {
  def offload(plan: SparkPlan): SparkPlan
}

object OffloadSingleNode {
  implicit class OffloadSingleNodeOps(rule: OffloadSingleNode) {

    /**
     * Converts the [[OffloadSingleNode]] rule to a strict version.
     *
     * In the strict version of the rule, all children of the input query plan node will be replaced
     * with 'DummyLeafExec' nodes so they are not accessible from the rule body.
     */
    def toStrcitRule(): OffloadSingleNode = {
      new StrictRule(rule);
    }
  }

  private class StrictRule(delegate: OffloadSingleNode) extends OffloadSingleNode {
    override def offload(plan: SparkPlan): SparkPlan = {
      val planWithChildrenHidden = hideChildren(plan)
      val applied = delegate.offload(planWithChildrenHidden)
      val out = restoreHiddenChildren(applied)
      out
    }

    /**
     * Replaces the children with 'DummyLeafExec' nodes so they become inaccessible afterward. Used
     * when the children plan nodes can be dropped because not interested.
     */
    private def hideChildren[T <: SparkPlan](plan: T): T = {
      plan
        .withNewChildren(
          plan.children.map {
            child =>
              val dummyLeaf = DummyLeafExec(child)
              child.logicalLink.foreach(dummyLeaf.setLogicalLink)
              dummyLeaf
          }
        )
        .asInstanceOf[T]
    }

    /** Restores hidden children from the replaced 'DummyLeafExec' nodes. */
    private def restoreHiddenChildren[T <: SparkPlan](plan: T): T = {
      plan
        .transformDown {
          case d: DummyLeafExec =>
            d.hiddenPlan
          case other => other
        }
        .asInstanceOf[T]
    }
  }

  /**
   * The plan node that hides the real child plan node during #applyOnNode call. This is used when
   * query planner doesn't allow a rule to access the child plan nodes from the input query plan
   * node.
   */
  private case class DummyLeafExec(hiddenPlan: SparkPlan) extends LeafExecNode with GlutenPlan {
    private lazy val conv: Convention = Convention.get(hiddenPlan)

    override def batchType(): Convention.BatchType = conv.batchType
    override def rowType0(): Convention.RowType = conv.rowType
    override def output: Seq[Attribute] = hiddenPlan.output
    override def outputPartitioning: Partitioning = hiddenPlan.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = hiddenPlan.outputOrdering

    override def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Not allowed in #applyOnNode call")
    override def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException("Not allowed in #applyOnNode call")
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException("Not allowed in #applyOnNode call")
  }
}
```

---

# plan

*This section contains 2 source files.*

## GlutenPlanModel.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/plan/GlutenPlanModel.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.plan

object GlutenPlanModel {
  def apply(): PlanModel[SparkPlan] = {
    PlanModelImpl
  }

  private object PlanModelImpl extends PlanModel[SparkPlan] {
    private val fakeTc = SparkTaskUtil.createTestTaskContext(new Properties())
    private def fakeTc[T](body: => T): T = {
      assert(!TaskResources.inSparkTask())
      SparkTaskUtil.setTaskContext(fakeTc)
      try {
        body
      } finally {
        SparkTaskUtil.unsetTaskContext()
      }
    }

    override def childrenOf(node: SparkPlan): Seq[SparkPlan] = node.children

    override def withNewChildren(node: SparkPlan, children: Seq[SparkPlan]): SparkPlan =
      node match {
        case c2r: ColumnarToRowExec =>
          // Workaround: To bypass the assertion in ColumnarToRowExec's code if child is
          // a group leaf.
          fakeTc {
            c2r.withNewChildren(children)
          }
        case other =>
          other.withNewChildren(children)
      }

    override def hashCode(node: SparkPlan): Int = Objects.hashCode(withEqualityWrapper(node))

    override def equals(one: SparkPlan, other: SparkPlan): Boolean =
      Objects.equals(withEqualityWrapper(one), withEqualityWrapper(other))

    override def newGroupLeaf(groupId: Int): GroupLeafExec.Builder =
      GroupLeafExec.newBuilder(groupId)

    override def isGroupLeaf(node: SparkPlan): Boolean = node match {
      case _: GroupLeafExec => true
      case _ => false
    }

    override def getGroupId(node: SparkPlan): Int = node match {
      case gl: GroupLeafExec => gl.groupId
      case _ => throw new IllegalStateException()
    }

    private def withEqualityWrapper(node: SparkPlan): AnyRef = node match {
      case scan: DataSourceV2ScanExecBase =>
        // Override V2 scan operators' equality implementation to include output attributes.
        //
        // Spark's V2 scans don't incorporate out attributes in equality so E.g.,
        // BatchScan[date#1] can be considered equal to BatchScan[date#2], which is unexpected
        // in RAS planner because it strictly relies on plan equalities for sanity.
        //
        // Related UT: `VeloxOrcDataTypeValidationSuite#Date type`
        // Related Spark PRs:
        // https://github.com/apache/spark/pull/23086
        // https://github.com/apache/spark/pull/23619
        // https://github.com/apache/spark/pull/23430
        ScanV2ExecEqualityWrapper(scan, scan.output)
      case other => other
    }

    private case class ScanV2ExecEqualityWrapper(
        scan: DataSourceV2ScanExecBase,
        output: Seq[Attribute])
  }
}
```

---

## GroupLeafExec.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/plan/GroupLeafExec.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.plan

// TODO: Make this inherit from GlutenPlan.
case class GroupLeafExec(groupId: Int, metadata: GlutenMetadata, convReq: Conv.Req)
  extends LeafExecNode
  with Convention.KnownBatchType
  with Convention.KnownRowTypeForSpark33OrLater
  with GlutenPlan.SupportsRowBasedCompatible {

  private val frozen = new AtomicBoolean(false)

  // Set the logical link then make the plan node immutable. All future
  // mutable operations related to tagging will be aborted.
  if (metadata.logicalLink() != LogicalLink.notFound) {
    setLogicalLink(metadata.logicalLink().plan)
  }
  frozen.set(true)

  override protected def doExecute(): RDD[InternalRow] = throw new IllegalStateException()
  override def output: Seq[Attribute] = metadata.schema().output

  override val batchType: Convention.BatchType = {
    val out = convReq.req.requiredBatchType match {
      case ConventionReq.BatchType.Any => Convention.BatchType.None
      case ConventionReq.BatchType.Is(b) => b
    }
    out
  }

  final override val supportsColumnar: Boolean = {
    batchType != Convention.BatchType.None
  }

  override val rowType0: Convention.RowType = {
    val out = convReq.req.requiredRowType match {
      case ConventionReq.RowType.Any => Convention.RowType.VanillaRowType
      case ConventionReq.RowType.Is(r) => r
    }
    out
  }

  final override val supportsRowBased: Boolean = {
    rowType() != Convention.RowType.None
  }

  private def ensureNotFrozen(): Unit = {
    if (frozen.get()) {
      throw new UnsupportedOperationException()
    }
  }

  // Enclose mutable APIs.
  override def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
    ensureNotFrozen()
    super.setLogicalLink(logicalPlan)
  }
  override def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    ensureNotFrozen()
    super.setTagValue(tag, value)
  }
  override def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    ensureNotFrozen()
    super.unsetTagValue(tag)
  }
  override def copyTagsFrom(other: SparkPlan): Unit = {
    ensureNotFrozen()
    super.copyTagsFrom(other)
  }
}

object GroupLeafExec {
  class Builder private[GroupLeafExec] (override val id: Int) extends GroupLeafBuilder[SparkPlan] {
    private var convReq: Conv.Req = _
    private var metadata: GlutenMetadata = _

    def withMetadata(metadata: GlutenMetadata): Builder = {
      this.metadata = metadata
      this
    }

    def withConvReq(convReq: Conv.Req): Builder = {
      this.convReq = convReq
      this
    }

    override def build(): SparkPlan = {
      require(metadata != null)
      require(convReq != null)
      GroupLeafExec(id, metadata, convReq)
    }
  }

  def newBuilder(groupId: Int): Builder = {
    new Builder(groupId)
  }
}
```

---

# planner

*This section contains 1 source files.*

## GlutenOptimization.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/GlutenOptimization.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner

object GlutenOptimization {
  def builder(): Builder = new BuilderImpl

  private object GlutenExplain extends RasExplain[SparkPlan] {
    override def describeNode(node: SparkPlan): String = node.nodeName
  }

  trait Builder {
    def addRules(rules: Seq[RasRule[SparkPlan]]): Builder
    def costModel(costModel: CostModel[SparkPlan]): Builder
    def create(): Optimization[SparkPlan]
  }

  private class BuilderImpl extends Builder {
    private val rules: mutable.ListBuffer[RasRule[SparkPlan]] = mutable.ListBuffer()
    private var costModel: Option[CostModel[SparkPlan]] = None

    override def addRules(rules: Seq[RasRule[SparkPlan]]): Builder = {
      this.rules ++= rules
      this
    }

    override def costModel(costModel: CostModel[SparkPlan]): Builder = {
      this.costModel = Some(costModel)
      this
    }

    override def create(): Optimization[SparkPlan] = {
      assert(costModel.isDefined, "Cost model is required to initialize GlutenOptimization")
      Optimization[SparkPlan](
        GlutenPlanModel(),
        costModel.get,
        GlutenMetadataModel(),
        GlutenPropertyModel(),
        GlutenExplain,
        RasRule.Factory.reuse(rules.toSeq))
    }
  }
}
```

---

# property

*This section contains 2 source files.*

## Conv.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/property/Conv.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.property

sealed trait Conv extends Property[SparkPlan] {
  override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
    ConvDef
  }
}

object Conv {
  val any: Conv = Req(ConventionReq.any)

  def of(conv: Convention): Prop = Prop(conv)
  def req(req: ConventionReq): Req = Req(req)

  def get(plan: SparkPlan): Prop = {
    Conv.of(Convention.get(plan))
  }

  def findTransition(from: Prop, to: Req): Transition = {
    val out = Transition.factory.findTransition(from.prop, to.req, new IllegalStateException())
    out
  }

  case class Prop(prop: Convention) extends Conv
  case class Req(req: ConventionReq) extends Conv {
    def isAny: Boolean = {
      req.requiredBatchType == ConventionReq.BatchType.Any &&
      req.requiredRowType == ConventionReq.RowType.Any
    }
  }
}

object ConvDef extends PropertyDef[SparkPlan, Conv] {
  // TODO: Should the convention-transparent ops (e.g., aqe shuffle read) support
  //  convention-propagation. Probably need to refactor getChildrenConstraints.
  override def getProperty(plan: SparkPlan): Conv = {
    conventionOf(plan)
  }

  private def conventionOf(plan: SparkPlan): Conv = {
    val out = Conv.get(plan)
    out
  }

  override def getChildrenConstraints(
      plan: SparkPlan,
      constraint: Property[SparkPlan]): Seq[Conv] = {
    val out = ConventionReq.get(plan).map(Conv.req)
    out
  }

  override def any(): Conv = Conv.any

  override def satisfies(
      property: Property[SparkPlan],
      constraint: Property[SparkPlan]): Boolean = {
    // The following enforces strict type checking against `property` and `constraint`
    // to make sure:
    //
    //  1. `property`, which came from user implementation of PropertyDef.getProperty, must be a
    //     `Prop`
    //  2. `constraint` which came from user implementation of PropertyDef.getChildrenConstraints,
    //     must be a `Req`
    //
    // If the user implementation doesn't follow the criteria, cast error will be thrown.
    //
    // This can be a common practice to implement a safe Property for RAS.
    //
    // TODO: Add a similar case to RAS UTs.
    (property, constraint) match {
      case (prop: Prop, req: Req) =>
        if (req.isAny) {
          return true
        }
        val out = Transition.factory.satisfies(prop.prop, req.req)
        out
    }
  }

  override def assignToGroup(
      group: GroupLeafBuilder[SparkPlan],
      constraint: Property[SparkPlan]): GroupLeafBuilder[SparkPlan] = (group, constraint) match {
    case (builder: GroupLeafExec.Builder, req: Req) =>
      builder.withConvReq(req)
  }
}

case class ConvEnforcerRule() extends EnforcerRuleFactory.SubRule[SparkPlan] {
  override def enforce(node: SparkPlan, constraint: Property[SparkPlan]): Iterable[SparkPlan] = {
    val reqConv = constraint.asInstanceOf[Req]
    val conv = Conv.get(node)
    if (ConvDef.satisfies(conv, reqConv)) {
      return List.empty
    }
    val transition = Conv.findTransition(conv, reqConv)
    val after = transition.apply(node)
    List(after)
  }
}
```

---

## GlutenPropertyModel.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/enumerated/planner/property/GlutenPropertyModel.scala`

```scala
package org.apache.gluten.extension.columnar.enumerated.planner.property

object GlutenPropertyModel {

  def apply(): PropertyModel[SparkPlan] = {
    PropertyModelImpl
  }

  private object PropertyModelImpl extends PropertyModel[SparkPlan] {
    override def propertyDefs: Seq[PropertyDef[SparkPlan, _ <: Property[SparkPlan]]] =
      Seq(ConvDef)

    override def newEnforcerRuleFactory(): EnforcerRuleFactory[SparkPlan] =
      EnforcerRuleFactory.fromSubRules(Seq(new EnforcerRuleFactory.SubRuleFactory[SparkPlan] {
        override def newSubRule(constraintDef: PropertyDef[SparkPlan, _ <: Property[SparkPlan]])
            : EnforcerRuleFactory.SubRule[SparkPlan] = {
          constraintDef match {
            case ConvDef => ConvEnforcerRule()
          }
        }

        override def ruleShape: Shape[SparkPlan] = Shapes.fixedHeight(1)
      }))
  }
}
```

---

# rewrite

*This section contains 1 source files.*

## RewriteSingleNode.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/rewrite/RewriteSingleNode.scala`

```scala
package org.apache.gluten.extension.columnar.rewrite

/**
 * Rewrites a plan node from vanilla Spark into its alternative representation.
 *
 * Gluten's planner will pick one that is considered the best executable plan between input plan and
 * the output plan.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 *
 * TODO: Ideally for such API we'd better to allow multiple alternative outputs.
 */
trait RewriteSingleNode {
  def isRewritable(plan: SparkPlan): Boolean
  def rewrite(plan: SparkPlan): SparkPlan
}
```

---

# shuffle

*This section contains 6 source files.*

## GlutenShuffleManager.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/shuffle/GlutenShuffleManager.scala`

```scala
package org.apache.spark.shuffle

/**
 * Shuffle manager that routes shuffle API calls to different shuffle managers registered by
 * different backends.
 *
 * A SPIP may cause refactoring of this class in the future:
 * https://issues.apache.org/jira/browse/SPARK-45792
 *
 * Experimental: This is not expected to be used in production yet. Use backend shuffle manager
 * (e.g., ColumnarShuffleManager or other RSS shuffle manager provided in Gluten's code
 * base)instead.
 */
@Experimental
class GlutenShuffleManager(conf: SparkConf, isDriver: Boolean) extends ShuffleManager {
  private val routerBuilder = ShuffleManagerRegistry.get().newRouterBuilder(conf, isDriver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    routerBuilder.getOrBuild().registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    routerBuilder.getOrBuild().getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    routerBuilder
      .getOrBuild()
      .getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    routerBuilder.getOrBuild().unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    routerBuilder.getOrBuild().shuffleBlockResolver
  }

  override def stop(): Unit = {
    routerBuilder.getOrBuild().stop()
  }
}
```

---

## LookupKey.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/shuffle/LookupKey.scala`

```scala
package org.apache.spark.shuffle

/**
 * Required during shuffle manager registration to determine whether the shuffle manager should be
 * used for the particular shuffle dependency.
 */
trait LookupKey {
  def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean
}
```

---

## ShuffleManagerLookup.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/shuffle/ShuffleManagerLookup.scala`

```scala
package org.apache.spark.shuffle

private class ShuffleManagerLookup(all: Seq[(LookupKey, ShuffleManager)]) {
  def findShuffleManager[K, V, C](dependency: ShuffleDependency[K, V, C]): ShuffleManager = {
    this.synchronized {
      // The latest shuffle manager registered will be looked up earlier.
      all.find(_._1.accepts(dependency)).map(_._2).getOrElse {
        throw new IllegalStateException(s"No ShuffleManager found for $dependency")
      }
    }
  }

  def all(): Seq[ShuffleManager] = {
    this.synchronized {
      all.map(_._2)
    }
  }
}
```

---

## ShuffleManagerRegistry.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/shuffle/ShuffleManagerRegistry.scala`

```scala
package org.apache.spark.shuffle

class ShuffleManagerRegistry private[ShuffleManagerRegistry] {
  private val all: mutable.ListBuffer[(LookupKey, String)] = mutable.ListBuffer()
  private val routerBuilders: mutable.Buffer[RouterBuilder] = mutable.Buffer()
  private val classDeDup: mutable.Set[String] = mutable.Set()

  // The shuffle manager class registered through this API later
  // will take higher precedence during lookup.
  def register(lookupKey: LookupKey, shuffleManagerClass: String): Unit = {
    val clazz = Utils.classForName(shuffleManagerClass)
    require(
      !clazz.isAssignableFrom(classOf[GlutenShuffleManager]),
      "It's not allowed to register GlutenShuffleManager recursively")
    require(
      classOf[ShuffleManager].isAssignableFrom(clazz),
      s"Shuffle manager class to register is not an implementation of Spark ShuffleManager: " +
        s"$shuffleManagerClass"
    )
    require(
      !classDeDup.contains(shuffleManagerClass),
      s"Shuffle manager class already registered: $shuffleManagerClass")
    this.synchronized {
      classDeDup += shuffleManagerClass
      (lookupKey -> shuffleManagerClass) +=: all
      // Invalidate all shuffle managers cached in each alive router builder instances.
      // Then, once the router builder is accessed, a new router will be forced to create.
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  // Visible for testing.
  private[shuffle] def clear(): Unit = {
    assert(SparkTestUtil.isTesting)
    this.synchronized {
      classDeDup.clear()
      all.clear()
      routerBuilders.foreach(_.invalidateCache())
    }
  }

  private[shuffle] def newRouterBuilder(conf: SparkConf, isDriver: Boolean): RouterBuilder =
    this.synchronized {
      val out = new RouterBuilder(this, conf, isDriver)
      routerBuilders += out
      out
    }
}

object ShuffleManagerRegistry {
  private val instance = {
    val r = new ShuffleManagerRegistry()
    r.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = {
          dependency.getClass == classOf[ShuffleDependency[_, _, _]]
        }
      },
      classOf[SortShuffleManager].getName
    )
    r
  }

  def get(): ShuffleManagerRegistry = instance

  class RouterBuilder(registry: ShuffleManagerRegistry, conf: SparkConf, isDriver: Boolean) {
    private var router: Option[ShuffleManagerRouter] = None

    private[ShuffleManagerRegistry] def invalidateCache(): Unit = synchronized {
      router = None
    }

    private[shuffle] def getOrBuild(): ShuffleManagerRouter = synchronized {
      if (router.isEmpty) {
        val instances = registry.all.map(key => key._1 -> instantiate(key._2, conf, isDriver))
        router = Some(new ShuffleManagerRouter(new ShuffleManagerLookup(instances.toSeq)))
      }
      router.get
    }

    private def instantiate(clazz: String, conf: SparkConf, isDriver: Boolean): ShuffleManager = {
      Utils
        .instantiateSerializerOrShuffleManager[ShuffleManager](clazz, conf, isDriver)
    }
  }
}
```

---

## ShuffleManagerRouter.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/shuffle/ShuffleManagerRouter.scala`

```scala
package org.apache.spark.shuffle
/** The internal shuffle manager instance used by GlutenShuffleManager. */
private class ShuffleManagerRouter(lookup: ShuffleManagerLookup)
  extends ShuffleManager
  with Logging {
  private val cache = new Cache()
  private val resolver = new BlockResolver(cache)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val manager = lookup.findShuffleManager(dependency)
    cache.store(shuffleId, manager).registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    ensureShuffleManagerRegistered(handle)
    cache.get(handle.shuffleId).getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    ensureShuffleManagerRegistered(handle)
    cache
      .get(handle.shuffleId)
      .getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    cache.remove(shuffleId).unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = resolver

  override def stop(): Unit = {
    if (!(cache.size() == 0)) {
      logWarning(
        s"Shuffle router cache is not empty when being stopped. This might be because the " +
          s"shuffle is not unregistered.")
    }
    lookup.all().foreach(_.stop())
  }

  private def ensureShuffleManagerRegistered(handle: ShuffleHandle): Unit = {
    val baseShuffleHandle = handle match {
      case b: BaseShuffleHandle[_, _, _] => b
      case _ =>
        throw new UnsupportedOperationException(
          s"${handle.getClass} is not a BaseShuffleHandle so is not supported by " +
            s"GlutenShuffleManager")
    }
    val shuffleId = baseShuffleHandle.shuffleId
    if (cache.has(shuffleId)) {
      return
    }
    val dependency = baseShuffleHandle.dependency
    val manager = lookup.findShuffleManager(dependency)
    cache.store(shuffleId, manager)
  }
}

private object ShuffleManagerRouter {
  private class Cache {
    private val cache: java.util.Map[Int, ShuffleManager] =
      new java.util.concurrent.ConcurrentHashMap()

    def has(shuffleId: Int): Boolean = {
      cache.containsKey(shuffleId)
    }

    def store(shuffleId: Int, manager: ShuffleManager): ShuffleManager = {
      cache.compute(
        shuffleId,
        (id, m) => {
          assert(m == null, s"Shuffle manager was already cached for shuffle id: $id")
          manager
        })
    }

    def get(shuffleId: Int): ShuffleManager = {
      val manager = cache.get(shuffleId)
      assert(manager != null, s"Shuffle manager not registered for shuffle id: $shuffleId")
      manager
    }

    def remove(shuffleId: Int): ShuffleManager = {
      val manager = cache.remove(shuffleId)
      assert(manager != null, s"Shuffle manager not registered for shuffle id: $shuffleId")
      manager
    }

    def size(): Int = {
      cache.size()
    }

    def clear(): Unit = {
      cache.clear()
    }
  }

  private class BlockResolver(cache: Cache) extends ShuffleBlockResolver {
    override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
      val shuffleId = blockId match {
        case id: ShuffleBlockId =>
          id.shuffleId
        case batchId: ShuffleBlockBatchId =>
          batchId.shuffleId
        case _ =>
          throw new IllegalArgumentException(
            "GlutenShuffleManager: Unsupported shuffle block id: " + blockId)
      }
      cache.get(shuffleId).shuffleBlockResolver.getBlockData(blockId, dirs)
    }

    override def getMergedBlockData(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
      val shuffleId = blockId.shuffleId
      cache.get(shuffleId).shuffleBlockResolver.getMergedBlockData(blockId, dirs)
    }

    override def getMergedBlockMeta(
        blockId: ShuffleMergedBlockId,
        dirs: Option[Array[String]]): MergedBlockMeta = {
      val shuffleId = blockId.shuffleId
      cache.get(shuffleId).shuffleBlockResolver.getMergedBlockMeta(blockId, dirs)
    }

    override def stop(): Unit = {
      throw new UnsupportedOperationException(
        s"BlockResolver ${getClass.getSimpleName} doesn't need to be explicitly stopped")
    }
  }
}
```

---

## GlutenShuffleManagerSuite.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/spark/shuffle/GlutenShuffleManagerSuite.scala`

```scala
package org.apache.spark.shuffle

class GlutenShuffleManagerSuite extends SharedSparkSession {
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(SHUFFLE_MANAGER.key, classOf[GlutenShuffleManager].getName)
      .set(UI_ENABLED, false)

  override protected def beforeEach(): Unit = {
    val registry = ShuffleManagerRegistry.get()
    registry.clear()
  }

  override protected def afterEach(): Unit = {
    val registry = ShuffleManagerRegistry.get()
    registry.clear()
    counter1.clear()
    counter2.clear()
  }

  test("register one") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("stop") == 0)
    gm.stop()
    assert(counter1.count("stop") == 1)
    gm.stop()
    gm.stop()
    assert(counter1.count("stop") == 3)
  }

  test("register two") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)
    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager2].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 0)
    // The statement calls #registerShuffle internally.
    val dep =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep.shuffleId)
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 1)

    assert(counter1.count("stop") == 0)
    assert(counter2.count("stop") == 0)
    gm.stop()
    assert(counter1.count("stop") == 1)
    assert(counter2.count("stop") == 1)
  }

  test("register two - disordered registration") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 0)
    val dep1 =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep1.shuffleId)
    assert(counter1.count("registerShuffle") == 1)
    assert(counter2.count("registerShuffle") == 0)

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager2].getName)

    // The statement calls #registerShuffle internally.
    val dep2 =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep2.shuffleId)
    assert(counter1.count("registerShuffle") == 1)
    assert(counter2.count("registerShuffle") == 1)

    assert(counter1.count("stop") == 0)
    assert(counter2.count("stop") == 0)
    gm.stop()
    assert(counter1.count("stop") == 1)
    assert(counter2.count("stop") == 1)
  }

  test("register two - with empty key") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)
    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = false
      },
      classOf[ShuffleManager2].getName)

    val gm = spark.sparkContext.env.shuffleManager
    assert(counter1.count("registerShuffle") == 0)
    assert(counter2.count("registerShuffle") == 0)
    // The statement calls #registerShuffle internally.
    val dep =
      new ShuffleDependency(new EmptyRDD[Product2[Any, Any]](spark.sparkContext), DummyPartitioner)
    gm.unregisterShuffle(dep.shuffleId)
    assert(counter1.count("registerShuffle") == 1)
    assert(counter2.count("registerShuffle") == 0)
  }

  test("register recursively") {
    val registry = ShuffleManagerRegistry.get()

    assertThrows[IllegalArgumentException](
      registry.register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[GlutenShuffleManager].getName))
  }

  test("register duplicated") {
    val registry = ShuffleManagerRegistry.get()

    registry.register(
      new LookupKey {
        override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
      },
      classOf[ShuffleManager1].getName)
    assertThrows[IllegalArgumentException](
      registry.register(
        new LookupKey {
          override def accepts[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = true
        },
        classOf[ShuffleManager1].getName))
  }
}

object GlutenShuffleManagerSuite {
  private val counter1 = new InvocationCounter
  private val counter2 = new InvocationCounter

  class ShuffleManager1(conf: SparkConf) extends ShuffleManager {
    private val delegate = new SortShuffleManager(conf)
    private val counter = counter1
    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      counter.increment("registerShuffle")
      delegate.registerShuffle(shuffleId, dependency)
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      counter.increment("getWriter")
      delegate.getWriter(handle, mapId, context, metrics)
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      counter.increment("getReader")
      delegate.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      counter.increment("unregisterShuffle")
      delegate.unregisterShuffle(shuffleId)
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      counter.increment("shuffleBlockResolver")
      delegate.shuffleBlockResolver
    }

    override def stop(): Unit = {
      counter.increment("stop")
      delegate.stop()
    }
  }

  class ShuffleManager2(conf: SparkConf, isDriver: Boolean) extends ShuffleManager {
    private val delegate = new SortShuffleManager(conf)
    private val counter = counter2
    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
      counter.increment("registerShuffle")
      delegate.registerShuffle(shuffleId, dependency)
    }

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
      counter.increment("getWriter")
      delegate.getWriter(handle, mapId, context, metrics)
    }

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
      counter.increment("getReader")
      delegate.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      counter.increment("unregisterShuffle")
      delegate.unregisterShuffle(shuffleId)
    }

    override def shuffleBlockResolver: ShuffleBlockResolver = {
      counter.increment("shuffleBlockResolver")
      delegate.shuffleBlockResolver
    }

    override def stop(): Unit = {
      counter.increment("stop")
      delegate.stop()
    }
  }

  private class InvocationCounter {
    private val counter: mutable.Map[String, AtomicInteger] = mutable.Map()

    def increment(name: String): Unit = synchronized {
      counter.getOrElseUpdate(name, new AtomicInteger()).incrementAndGet()
    }

    def count(name: String): Int = {
      counter.getOrElse(name, new AtomicInteger()).get()
    }

    def clear(): Unit = {
      counter.clear()
    }
  }

  private object DummyPartitioner extends Partitioner {
    override def numPartitions: Int = 0
    override def getPartition(key: Any): Int = 0
  }
}
```

---

# spark

*This section contains 4 source files.*

## RegularMemoryConsumer.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/RegularMemoryConsumer.java`

```java
package org.apache.gluten.memory.memtarget.spark;

/**
 * A trivial memory consumer implementation used by Gluten.
 *
 * @deprecated Use {@link TreeMemoryConsumers} instead.
 */
@Deprecated
public class RegularMemoryConsumer extends MemoryConsumer
    implements MemoryTarget, KnownNameAndStats {

  private final TaskMemoryManager taskMemoryManager;
  private final Spiller spiller;
  private final String name;
  private final Map<String, MemoryUsageStatsBuilder> virtualChildren;
  private final MemoryUsageRecorder selfRecorder = new SimpleMemoryUsageRecorder();

  public RegularMemoryConsumer(
      TaskMemoryManager taskMemoryManager,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
    this.taskMemoryManager = taskMemoryManager;
    this.spiller = spiller;
    this.name = MemoryTargetUtil.toUniqueName("Gluten.Regular." + name);
    this.virtualChildren = virtualChildren;
  }

  @Override
  public long spill(final long size, MemoryConsumer trigger) {
    long remainingBytes = size;
    for (Spiller.Phase phase : Spiller.Phase.values()) {
      // First shrink, then if no good, spill.
      if (remainingBytes <= 0) {
        break;
      }
      remainingBytes -= spiller.spill(this, phase, size);
    }
    long spilledOut = size - remainingBytes;
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMetrics().incMemoryBytesSpilled(spilledOut);
    }
    return spilledOut;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public MemoryUsageStats stats() {
    MemoryUsageStats stats = this.selfRecorder.toStats();
    Preconditions.checkState(
        stats.getCurrent() == getUsed(),
        "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
    // In the case of Velox backend, stats returned from C++ (as the singleton virtual children,
    // currently) may not include all allocations according to the way collectMemoryUsage()
    // is implemented. So we add them as children of this consumer's stats
    return MemoryUsageStats.newBuilder()
        .setCurrent(stats.getCurrent())
        .setPeak(stats.getPeak())
        .putAllChildren(
            virtualChildren.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toStats())))
        .build();
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      // or Spark complains about the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    selfRecorder.inc(acquired);
    return acquired;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    long toFree = Math.min(getUsed(), size);
    freeMemory(toFree);
    Preconditions.checkArgument(getUsed() >= 0);
    selfRecorder.inc(-toFree);
    return toFree;
  }

  @Override
  public String toString() {
    return name();
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }
}
```

---

## TreeMemoryConsumer.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumer.java`

```java
package org.apache.gluten.memory.memtarget.spark;

/**
 * This is a Spark memory consumer and at the same time a factory to create sub-targets that share
 * one fixed memory capacity.
 *
 * <p>Once full (size > capacity), spillers will be called by the consumer with order. If failed to
 * free out enough memory, throw OOM to caller.
 *
 * <p>Spark's memory manager could either trigger spilling on the children spillers since this was
 * registered as a Spark memory consumer.
 *
 * <p>Typically used by utility class {@link
 * org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumers}.
 */
public class TreeMemoryConsumer extends MemoryConsumer implements TreeMemoryTarget {

  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final Map<String, TreeMemoryTarget> children = new HashMap<>();
  private final String name = MemoryTargetUtil.toUniqueName("Gluten.Tree");

  TreeMemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), mode);
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      // or Spark complains about the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    recorder.inc(acquired);
    return acquired;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    long toFree = Math.min(getUsed(), size);
    freeMemory(toFree);
    Preconditions.checkArgument(getUsed() >= 0);
    recorder.inc(-toFree);
    return toFree;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public MemoryUsageStats stats() {
    Set<Map.Entry<String, TreeMemoryTarget>> entries = children.entrySet();
    Map<String, MemoryUsageStats> childrenStats =
        entries.stream()
            .collect(Collectors.toMap(e -> e.getValue().name(), e -> e.getValue().stats()));

    Preconditions.checkState(childrenStats.size() == children.size());
    MemoryUsageStats stats = recorder.toStats(childrenStats);
    Preconditions.checkState(
        stats.getCurrent() == getUsed(),
        "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
    return stats;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    // subject to the regular Spark spill calls
    return TreeMemoryTargets.spillTree(this, size);
  }

  @Override
  public TreeMemoryTarget newChild(
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    final TreeMemoryTarget child = new Node(this, name, capacity, spiller, virtualChildren);
    if (children.containsKey(child.name())) {
      throw new IllegalArgumentException("Child already registered: " + child.name());
    }
    children.put(child.name(), child);
    return child;
  }

  @Override
  public Map<String, TreeMemoryTarget> children() {
    return Collections.unmodifiableMap(children);
  }

  @Override
  public TreeMemoryTarget parent() {
    // we are root
    throw new IllegalStateException(
        "Unreachable code org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumer.parent");
  }

  @Override
  public Spiller getNodeSpiller() {
    // root doesn't spill
    return Spillers.NOOP;
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }

  public static class Node implements TreeMemoryTarget, KnownNameAndStats {
    private final Map<String, Node> children = new HashMap<>();
    private final TreeMemoryTarget parent;
    private final String name;
    private final long capacity;
    private final Spiller spiller;
    private final Map<String, MemoryUsageStatsBuilder> virtualChildren;
    private final SimpleMemoryUsageRecorder selfRecorder = new SimpleMemoryUsageRecorder();

    private Node(
        TreeMemoryTarget parent,
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      this.parent = parent;
      this.capacity = capacity;
      final String uniqueName = MemoryTargetUtil.toUniqueName(name);
      if (capacity == TreeMemoryTarget.CAPACITY_UNLIMITED) {
        this.name = uniqueName;
      } else {
        this.name = String.format("%s, %s", uniqueName, Utils.bytesToString(capacity));
      }
      this.spiller = spiller;
      this.virtualChildren = virtualChildren;
    }

    @Override
    public long borrow(long size) {
      if (size == 0) {
        return 0;
      }
      ensureFreeCapacity(size);
      return borrow0(Math.min(freeBytes(), size));
    }

    private long freeBytes() {
      return capacity - usedBytes();
    }

    private long borrow0(long size) {
      long granted = parent.borrow(size);
      selfRecorder.inc(granted);
      return granted;
    }

    @Override
    public Spiller getNodeSpiller() {
      return spiller;
    }

    private boolean ensureFreeCapacity(long bytesNeeded) {
      while (true) { // FIXME should we add retry limit?
        long freeBytes = freeBytes();
        Preconditions.checkState(freeBytes >= 0);
        if (freeBytes >= bytesNeeded) {
          // free bytes fit requirement
          return true;
        }
        // spill
        long bytesToSpill = bytesNeeded - freeBytes;
        long spilledBytes = TreeMemoryTargets.spillTree(this, bytesToSpill);
        Preconditions.checkState(spilledBytes >= 0);
        if (spilledBytes == 0) {
          // OOM
          return false;
        }
      }
    }

    @Override
    public long repay(long size) {
      if (size == 0) {
        return 0;
      }
      long toFree = Math.min(usedBytes(), size);
      long freed = parent.repay(toFree);
      selfRecorder.inc(-freed);
      return freed;
    }

    @Override
    public long usedBytes() {
      return selfRecorder.current();
    }

    @Override
    public <T> T accept(MemoryTargetVisitor<T> visitor) {
      return visitor.visit(this);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public MemoryUsageStats stats() {
      final Map<String, MemoryUsageStats> childrenStats =
          new HashMap<>(
              children.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getValue().name(), e -> e.getValue().stats())));

      Preconditions.checkState(childrenStats.size() == children.size());

      // add virtual children
      for (Map.Entry<String, MemoryUsageStatsBuilder> entry : virtualChildren.entrySet()) {
        if (childrenStats.containsKey(entry.getKey())) {
          throw new IllegalArgumentException("Child stats already exists: " + entry.getKey());
        }
        childrenStats.put(entry.getKey(), entry.getValue().toStats());
      }
      return selfRecorder.toStats(childrenStats);
    }

    @Override
    public TreeMemoryTarget newChild(
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      final Node child =
          new Node(this, name, Math.min(this.capacity, capacity), spiller, virtualChildren);
      if (children.containsKey(child.name())) {
        throw new IllegalArgumentException("Child already registered: " + child.name());
      }
      children.put(child.name(), child);
      return child;
    }

    @Override
    public Map<String, TreeMemoryTarget> children() {
      return Collections.unmodifiableMap(children);
    }

    @Override
    public TreeMemoryTarget parent() {
      return parent;
    }
  }
}
```

---

## TreeMemoryConsumers.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumers.java`

```java
package org.apache.gluten.memory.memtarget.spark;

public final class TreeMemoryConsumers {
  private static final ReferenceMap FACTORIES = new ReferenceMap();

  private TreeMemoryConsumers() {}

  @SuppressWarnings("unchecked")
  public static Factory factory(TaskMemoryManager tmm, MemoryMode mode) {
    synchronized (FACTORIES) {
      final Factory factory =
          (Factory) FACTORIES.computeIfAbsent(tmm, m -> new Factory((TaskMemoryManager) m, mode));
      final MemoryMode foundMode = factory.sparkConsumer.getMode();
      Preconditions.checkState(
          foundMode == mode,
          "An existing Spark memory consumer already exists but is of the different memory "
              + "mode: %s",
          foundMode);
      return factory;
    }
  }

  public static class Factory {
    private final TreeMemoryConsumer sparkConsumer;
    private final Map<Long, TreeMemoryTarget> roots = new ConcurrentHashMap<>();

    private Factory(TaskMemoryManager tmm, MemoryMode mode) {
      this.sparkConsumer = new TreeMemoryConsumer(tmm, mode);
    }

    private TreeMemoryTarget ofCapacity(long capacity) {
      return roots.computeIfAbsent(
          capacity,
          cap ->
              sparkConsumer.newChild(
                  String.format("Capacity[%s]", Utils.bytesToString(cap)),
                  cap,
                  Spillers.NOOP,
                  Collections.emptyMap()));
    }

    /**
     * This works as a legacy Spark memory consumer which grants as much as possible of memory
     * capacity to each task.
     */
    public TreeMemoryTarget legacyRoot() {
      return ofCapacity(TreeMemoryTarget.CAPACITY_UNLIMITED);
    }

    /**
     * A hub to provide memory target instances whose shared size (in the same task) is limited to
     * X, X = executor memory / task slots.
     *
     * <p>Using this to prevent OOMs if the delegated memory target could possibly hold large memory
     * blocks that are not spill-able.
     *
     * <p>See <a href="https://github.com/oap-project/gluten/issues/3030">GLUTEN-3030</a>
     */
    public TreeMemoryTarget isolatedRoot() {
      return ofCapacity(GlutenCoreConfig.get().conservativeTaskOffHeapMemorySize());
    }
  }
}
```

---

## TreeMemoryConsumerTest.java

**Path**: `../incubator-gluten/gluten-core/src/test/java/org/apache/gluten/memory/memtarget/spark/TreeMemoryConsumerTest.java`

```java
package org.apache.gluten.memory.memtarget.spark;

public class TreeMemoryConsumerTest {
  @Before
  public void setUp() throws Exception {
    final SQLConf conf = SQLConf.get();
    conf.setConfString("spark.memory.offHeap.enabled", "true");
    conf.setConfString("spark.memory.offHeap.size", "400");
    conf.setConfString(
        GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(), "100");
  }

  @Test
  public void testIsolated() {
    test(
        () -> {
          final TreeMemoryConsumers.Factory factory =
              TreeMemoryConsumers.factory(
                  TaskContext.get().taskMemoryManager(), MemoryMode.OFF_HEAP);
          final TreeMemoryTarget consumer =
              factory
                  .isolatedRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(10, consumer.borrow(20));
          Assert.assertEquals(0, consumer.borrow(20));
        });
  }

  @Test
  public void testLegacy() {
    test(
        () -> {
          final TreeMemoryConsumers.Factory factory =
              TreeMemoryConsumers.factory(
                  TaskContext.get().taskMemoryManager(), MemoryMode.OFF_HEAP);
          final TreeMemoryTarget consumer =
              factory
                  .legacyRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(20, consumer.borrow(20));
        });
  }

  @Test
  public void testIsolatedAndLegacy() {
    test(
        () -> {
          final TreeMemoryTarget legacy =
              TreeMemoryConsumers.factory(
                      TaskContext.get().taskMemoryManager(), MemoryMode.OFF_HEAP)
                  .legacyRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(110, legacy.borrow(110));
          final TreeMemoryTarget isolated =
              TreeMemoryConsumers.factory(
                      TaskContext.get().taskMemoryManager(), MemoryMode.OFF_HEAP)
                  .isolatedRoot()
                  .newChild(
                      "FOO",
                      TreeMemoryTarget.CAPACITY_UNLIMITED,
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(100, isolated.borrow(110));
        });
  }

  @Test
  public void testSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget legacy =
              TreeMemoryConsumers.factory(
                      TaskContext.get().taskMemoryManager(), MemoryMode.OFF_HEAP)
                  .legacyRoot()
                  .newChild(
                      "FOO", TreeMemoryTarget.CAPACITY_UNLIMITED, spillers, Collections.emptyMap());
          final AtomicInteger numSpills = new AtomicInteger(0);
          final AtomicLong numSpilledBytes = new AtomicLong(0L);
          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  long repaid = legacy.repay(size);
                  numSpills.getAndIncrement();
                  numSpilledBytes.getAndAdd(repaid);
                  return repaid;
                }
              });
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(1, numSpills.get());
          Assert.assertEquals(200, numSpilledBytes.get());
          Assert.assertEquals(400, legacy.usedBytes());

          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(3, numSpills.get());
          Assert.assertEquals(800, numSpilledBytes.get());
          Assert.assertEquals(400, legacy.usedBytes());
        });
  }

  @Test
  public void testOverSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget legacy =
              TreeMemoryConsumers.factory(
                      TaskContext.get().taskMemoryManager(), MemoryMode.OFF_HEAP)
                  .legacyRoot()
                  .newChild(
                      "FOO", TreeMemoryTarget.CAPACITY_UNLIMITED, spillers, Collections.emptyMap());
          final AtomicInteger numSpills = new AtomicInteger(0);
          final AtomicLong numSpilledBytes = new AtomicLong(0L);
          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  long repaid = legacy.repay(Long.MAX_VALUE);
                  numSpills.getAndIncrement();
                  numSpilledBytes.getAndAdd(repaid);
                  return repaid;
                }
              });
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(1, numSpills.get());
          Assert.assertEquals(300, numSpilledBytes.get());
          Assert.assertEquals(300, legacy.usedBytes());

          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(300, legacy.borrow(300));
          Assert.assertEquals(3, numSpills.get());
          Assert.assertEquals(900, numSpilledBytes.get());
          Assert.assertEquals(300, legacy.usedBytes());
        });
  }

  private void test(Runnable r) {
    TaskResources$.MODULE$.runUnsafe(
        new Function0<Object>() {
          @Override
          public Object apply() {
            r.run();
            return null;
          }
        });
  }
}
```

---

# storage

*This section contains 1 source files.*

## BlockManagerUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/storage/BlockManagerUtil.scala`

```scala
package org.apache.spark.storage

object BlockManagerUtil {
  def setTestMemoryStore(conf: SparkConf, memoryManager: MemoryManager, isDriver: Boolean): Unit = {
    val store = new MemoryStore(
      conf,
      new BlockInfoManager,
      new SerializerManager(
        Utils.instantiateSerializerFromConf[Serializer](SERIALIZER, conf, isDriver),
        conf),
      memoryManager,
      new BlockEvictionHandler {
        override private[storage] def dropFromMemory[T: ClassTag](
            blockId: BlockId,
            data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
          throw new UnsupportedOperationException(
            s"Cannot drop block ID $blockId from test memory store")
        }
      }
    )
    memoryManager.setMemoryStore(store)
  }
}
```

---

# task

*This section contains 4 source files.*

## TaskListener.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/task/TaskListener.scala`

```scala
package org.apache.gluten.task

trait TaskListener {
  def onTaskStart(): Unit
  def onTaskSucceeded(): Unit
  def onTaskFailed(failureReason: TaskFailedReason): Unit
}
```

---

## TaskResource.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/task/TaskResource.scala`

```scala
package org.apache.spark.task

/**
 * Manages the lifecycle for a specific type of memory resource managed by Spark. See also
 * `org.apache.spark.util.TaskResources`.
 */
trait TaskResource {
  @throws(classOf[Exception])
  def release(): Unit

  // #release() will be called in higher precedence if the manager has higher priority
  def priority(): Int = 100

  def resourceName(): String
}
```

---

## TaskResources.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/task/TaskResources.scala`

```scala
package org.apache.spark.task

object TaskResources extends TaskListener with Logging {
  // And open java assert mode to get memory stack
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true")
      .toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  private def newUnsafeTaskContext(properties: Properties): TaskContext = {
    SparkTaskUtil.createTestTaskContext(properties)
  }

  implicit private class PropertiesOps(properties: Properties) {
    def setIfMissing(key: String, value: String): Unit = {
      if (!properties.containsKey(key)) {
        properties.setProperty(key, value)
      }
    }
  }

  private def setUnsafeTaskContext(): Unit = {
    if (inSparkTask()) {
      throw new UnsupportedOperationException(
        "TaskResources#runUnsafe should only be used outside Spark task")
    }
    val properties = new Properties()
    SQLConf.get.getAllConfs.foreach {
      case (key, value) if key.startsWith("spark") =>
        properties.put(key, value)
      case _ =>
    }
    properties.setIfMissing(GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY, "true")
    properties.setIfMissing(GlutenCoreConfig.SPARK_OFFHEAP_SIZE_KEY, "1TB")
    TaskContext.setTaskContext(newUnsafeTaskContext(properties))
  }

  private def unsetUnsafeTaskContext(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException()
    }
    if (getLocalTaskContext().taskAttemptId() != -1) {
      throw new IllegalStateException()
    }
    TaskContext.unset()
  }

  // Run code with unsafe task context. If the call took place from Spark driver or test code
  // without a Spark task context registered, a temporary unsafe task context instance will
  // be created and used. Since unsafe task context is not managed by Spark's task memory manager,
  // Spark may not be aware of the allocations happened inside the user code.
  //
  // The API should only be used in the following cases:
  //
  // 1. Run code on driver
  // 2. Run test code
  def runUnsafe[T](body: => T): T = {
    TaskResources.setUnsafeTaskContext()
    onTaskStart()
    val context = getLocalTaskContext()
    try {
      val out =
        try {
          body
        } catch {
          case t: Throwable =>
            // Similar code with those in Task.scala
            try {
              context.markTaskFailed(t)
            } catch {
              case t: Throwable =>
                t.addSuppressed(t)
            }
            context.markTaskCompleted(Some(t))
            throw t
        } finally {
          try {
            context.markTaskCompleted(None)
          } finally {
            TaskResources.unsetUnsafeTaskContext()
          }
        }
      onTaskSucceeded()
      out
    } catch {
      case t: Throwable =>
        onTaskFailed(UnknownReason)
        throw t
    }
  }

  private val RESOURCE_REGISTRIES =
    new java.util.IdentityHashMap[TaskContext, TaskResourceRegistry]()

  def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }

  def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  private def getTaskResourceRegistry(): TaskResourceRegistry = {
    if (!inSparkTask()) {
      throw new UnsupportedOperationException(
        "Not in a Spark task. If the code is running on driver or for testing purpose, " +
          "try using TaskResources#runUnsafe")
    }
    val tc = getLocalTaskContext()
    RESOURCE_REGISTRIES.synchronized {
      if (!RESOURCE_REGISTRIES.containsKey(tc)) {
        throw new IllegalStateException(
          "" +
            "TaskResourceRegistry is not initialized, please ensure TaskResources " +
            "is added to GlutenExecutorPlugin's task listener list")
      }
      return RESOURCE_REGISTRIES.get(tc)
    }
  }

  def addRecycler(name: String, prio: Int)(f: => Unit): Unit = {
    addAnonymousResource(new TaskResource {
      override def release(): Unit = f

      override def priority(): Int = prio

      override def resourceName(): String = name
    })
  }

  def addResource[T <: TaskResource](id: String, resource: T): T = {
    getTaskResourceRegistry().addResource(id, resource)
  }

  def releaseResource(id: String): Unit = {
    getTaskResourceRegistry().releaseResource(id)
  }

  def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T = {
    getTaskResourceRegistry().addResourceIfNotRegistered(id, factory)
  }

  def addAnonymousResource[T <: TaskResource](resource: T): T = {
    getTaskResourceRegistry().addResource(UUID.randomUUID().toString, resource)
  }

  def isResourceRegistered(id: String): Boolean = {
    getTaskResourceRegistry().isResourceRegistered(id)
  }

  def getResource[T <: TaskResource](id: String): T = {
    getTaskResourceRegistry().getResource(id)
  }

  def getSharedUsage(): SimpleMemoryUsageRecorder = {
    getTaskResourceRegistry().getSharedUsage()
  }

  override def onTaskStart(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    val tc = getLocalTaskContext()
    RESOURCE_REGISTRIES.synchronized {
      if (RESOURCE_REGISTRIES.containsKey(tc)) {
        throw new IllegalStateException(
          "TaskResourceRegistry is already initialized, this should not happen")
      }
      val registry = new TaskResourceRegistry
      RESOURCE_REGISTRIES.put(tc, registry)
      tc.addTaskFailureListener(
        // in case of crashing in task completion listener, errors may be swallowed
        new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            // TODO:
            // The general duty of printing error message should not reside in memory module
            error match {
              case e: TaskKilledException if e.reason == "another attempt succeeded" =>
              case _ => logError(s"Task ${context.taskAttemptId()} failed by error: ", error)
            }
          }
        })
      tc.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          RESOURCE_REGISTRIES.synchronized {
            val currentTaskRegistries = RESOURCE_REGISTRIES.get(context)
            if (currentTaskRegistries == null) {
              throw new IllegalStateException(
                "TaskResourceRegistry is not initialized, this should not happen")
            }
            // We should first call `releaseAll` then remove the registries, because
            // the functions inside registries may register new resource to registries.
            currentTaskRegistries.releaseAll()
            context.taskMetrics().incPeakExecutionMemory(registry.getSharedUsage().peak())
            RESOURCE_REGISTRIES.remove(context)
          }
        }
      })
    }
  }

  private def onTaskExit(): Unit = {
    // no-op
  }

  override def onTaskSucceeded(): Unit = {
    onTaskExit()
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    onTaskExit()
  }
}

// thread safe
class TaskResourceRegistry extends Logging {
  private val sharedUsage = new SimpleMemoryUsageRecorder()
  private val resources = new util.HashMap[String, TaskResource]()
  private val priorityToResourcesMapping: util.HashMap[Int, util.LinkedHashSet[TaskResource]] =
    new util.HashMap[Int, util.LinkedHashSet[TaskResource]]()

  private var exclusiveLockAcquired: Boolean = false
  private def lock[T](body: => T): T = {
    synchronized {
      if (exclusiveLockAcquired) {
        throw new ConcurrentModificationException
      }
      body
    }
  }
  private def exclusiveLock[T](body: => T): T = {
    synchronized {
      if (exclusiveLockAcquired) {
        throw new ConcurrentModificationException
      }
      exclusiveLockAcquired = true
      try {
        body
      } finally {
        exclusiveLockAcquired = false
      }
    }
  }

  private def addResource0(id: String, resource: TaskResource): Unit = lock {
    resources.put(id, resource)
    priorityToResourcesMapping
      .computeIfAbsent(resource.priority(), _ => new util.LinkedHashSet[TaskResource]())
      .add(resource)
  }

  private def release(resource: TaskResource): Unit = exclusiveLock {
    // We disallow modification on registry's members when calling the user-defined release code.
    resource.release()
  }

  /** Release all managed resources according to priority and reversed order */
  private[task] def releaseAll(): Unit = lock {
    val table = new util.ArrayList(priorityToResourcesMapping.entrySet())
    Collections.sort(
      table,
      (
          o1: util.Map.Entry[Int, util.LinkedHashSet[TaskResource]],
          o2: util.Map.Entry[Int, util.LinkedHashSet[TaskResource]]) => {
        val diff = o2.getKey - o1.getKey // descending by priority
        if (diff > 0) {
          1
        } else if (diff < 0) {
          -1
        } else {
          throw new IllegalStateException(
            "Unreachable code from org.apache.spark.task.TaskResourceRegistry.releaseAll")
        }
      }
    )
    table.forEach {
      _.getValue.asScala.toSeq.reverse
        .foreach(release(_)) // lifo for all resources within the same priority
    }
    priorityToResourcesMapping.clear()
    resources.clear()
  }

  /** Release single resource by ID */
  private[task] def releaseResource(id: String): Unit = lock {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    val resource = resources.get(id)
    if (!priorityToResourcesMapping.containsKey(resource.priority())) {
      throw new IllegalStateException("TaskResource's priority not found in priority mapping")
    }
    val samePrio = priorityToResourcesMapping.get(resource.priority())
    if (!samePrio.contains(resource)) {
      throw new IllegalStateException("TaskResource not found in priority mapping")
    }
    release(resource)
    samePrio.remove(resource)
    resources.remove(id)
  }

  private[task] def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T =
    lock {
      if (resources.containsKey(id)) {
        return resources.get(id).asInstanceOf[T]
      }
      val resource = factory.apply()
      addResource0(id, resource)
      resource
    }

  private[task] def addResource[T <: TaskResource](id: String, resource: T): T = lock {
    if (resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is already registered", id))
    }
    addResource0(id, resource)
    resource
  }

  private[task] def isResourceRegistered(id: String): Boolean = lock {
    resources.containsKey(id)
  }

  private[task] def getResource[T <: TaskResource](id: String): T = lock {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    resources.get(id).asInstanceOf[T]
  }

  private[task] def getSharedUsage(): SimpleMemoryUsageRecorder = lock {
    sharedUsage
  }
}
```

---

## TaskResourceSuite.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/gluten/task/TaskResourceSuite.scala`

```scala
package org.apache.gluten.task

class TaskResourceSuite extends AnyFunSuite with SQLHelper {
  test("Run unsafe") {
    val out = TaskResources.runUnsafe {
      1
    }
    assert(out == 1)
  }

  test("Run unsafe - task context") {
    TaskResources.runUnsafe {
      assert(TaskResources.inSparkTask())
      assert(TaskResources.getLocalTaskContext() != null)
    }
  }

  test("Run unsafe - propagate Spark config") {
    val total = 128 * 1024 * 1024
    withSQLConf(
      "spark.memory.offHeap.enabled" -> "true",
      "spark.memory.offHeap.size" -> s"$total",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      TaskResources.runUnsafe {
        assert(TaskResources.inSparkTask())
        assert(TaskResources.getLocalTaskContext() != null)

        val tmm = SparkTaskUtil.getTaskMemoryManager(TaskResources.getLocalTaskContext())
        val consumer = new MemoryConsumer(tmm, MemoryMode.OFF_HEAP) {
          override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
        }
        assert(consumer.acquireMemory(total) == total)
        assert(consumer.acquireMemory(1) == 0)

        assert(!SQLConf.get.adaptiveExecutionEnabled)
      }
    }
  }

  test("Run unsafe - register resource") {
    var unregisteredCount = 0
    TaskResources.runUnsafe {
      TaskResources.addResource(
        UUID.randomUUID().toString,
        new TaskResource {
          override def release(): Unit = unregisteredCount += 1

          override def resourceName(): String = "test resource 1"
        })
      TaskResources.addResource(
        UUID.randomUUID().toString,
        new TaskResource {
          override def release(): Unit = unregisteredCount += 1

          override def resourceName(): String = "test resource 2"
        })
    }
    assert(unregisteredCount == 2)
  }
}
```

---

# transition

*This section contains 9 source files.*

## Convention.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Convention.scala`

```scala
package org.apache.gluten.extension.columnar.transition

/**
 * Convention of a query plan consists of the row data type and columnar data type it supports to
 * output.
 */
sealed trait Convention {
  def rowType: Convention.RowType
  def batchType: Convention.BatchType
}

object Convention {
  def ensureSparkRowAndBatchTypesRegistered(): Unit = {
    RowType.None.ensureRegistered()
    RowType.VanillaRowType.ensureRegistered()
    BatchType.None.ensureRegistered()
    BatchType.VanillaBatchType.ensureRegistered()
  }

  implicit class ConventionOps(val conv: Convention) extends AnyVal {
    def isNone: Boolean = {
      conv.rowType == RowType.None && conv.batchType == BatchType.None
    }

    def &&(other: Convention): Convention = {
      def rowType(): RowType = {
        if (conv.rowType == other.rowType) {
          return conv.rowType
        }
        RowType.None
      }
      def batchType(): BatchType = {
        if (conv.batchType == other.batchType) {
          return conv.batchType
        }
        BatchType.None
      }
      Convention.of(rowType(), batchType())
    }

    def asReq(): ConventionReq = {
      val rowTypeReq = conv.rowType match {
        case Convention.RowType.None => ConventionReq.RowType.Any
        case r => ConventionReq.RowType.Is(r)
      }

      val batchTypeReq = conv.batchType match {
        case Convention.BatchType.None => ConventionReq.BatchType.Any
        case b => ConventionReq.BatchType.Is(b)
      }
      ConventionReq.of(rowTypeReq, batchTypeReq)
    }
  }

  private case class Impl(override val rowType: RowType, override val batchType: BatchType)
    extends Convention

  def get(plan: SparkPlan): Convention = {
    ConventionFunc.create().conventionOf(plan)
  }

  def of(rowType: RowType, batchType: BatchType): Convention = {
    Impl(rowType, batchType)
  }

  trait RowOrBatchType extends TransitionGraph.Vertex {

    /**
     * User row / batch type could override this method to define transitions from/to this batch
     * type by calling the subsequent protected APIs.
     */
    protected[this] def registerTransitions(): Unit

    final protected[this] def fromRow(from: RowType, transition: Transition): Unit = {
      assert(from != this)
      Transition.factory.update(graph => graph.addEdge(from, this, transition))
    }

    final protected[this] def toRow(to: RowType, transition: Transition): Unit = {
      assert(to != this)
      Transition.factory.update(graph => graph.addEdge(this, to, transition))
    }

    final protected[this] def fromBatch(from: BatchType, transition: Transition): Unit = {
      assert(from != this)
      Transition.factory.update(graph => graph.addEdge(from, this, transition))
    }

    final protected[this] def toBatch(to: BatchType, transition: Transition): Unit = {
      assert(to != this)
      Transition.factory.update(graph => graph.addEdge(this, to, transition))
    }
  }

  trait RowType extends RowOrBatchType with Serializable {
    final protected[this] def register0(): Unit = BatchType.synchronized {
      assert(all.add(this))
      registerTransitions()
    }
  }

  object RowType {
    private val all: mutable.Set[RowType] = mutable.Set()
    def values(): Set[RowType] = all.toSet

    // None indicates that the plan doesn't support row-based processing.
    final case object None extends RowType {
      override protected[this] def registerTransitions(): Unit = {}
    }
    final case object VanillaRowType extends RowType {
      override protected[this] def registerTransitions(): Unit = {}
    }
  }

  trait BatchType extends RowOrBatchType with Serializable {
    final protected[this] def register0(): Unit = BatchType.synchronized {
      assert(all.add(this))
      registerTransitions()
    }
  }

  object BatchType {
    private val all: mutable.Set[BatchType] = mutable.Set()
    def values(): Set[BatchType] = all.toSet
    // None indicates that the plan doesn't support batch-based processing.
    final case object None extends BatchType {
      override protected[this] def registerTransitions(): Unit = {}
    }
    final case object VanillaBatchType extends BatchType {
      override protected[this] def registerTransitions(): Unit = {
        fromRow(RowType.VanillaRowType, RowToColumnarExec.apply)
        toRow(RowType.VanillaRowType, ColumnarToRowExec.apply)
      }
    }
  }

  trait KnownBatchType {
    def batchType(): BatchType
  }

  sealed trait KnownRowType {
    def rowType(): RowType
  }

  trait KnownRowTypeForSpark33OrLater extends KnownRowType {
    this: SparkPlan =>

    final override def rowType(): RowType = {
      if (SparkVersionUtil.lteSpark32) {
        // It's known that in Spark 3.2, one Spark plan node is considered either only having
        // row-based support or only having columnar support at a time.
        // Hence, if the plan supports columnar output, we'd disable its row-based support.
        // The same for the opposite.
        if (supportsColumnar) {
          Convention.RowType.None
        } else {
          assert(rowType0() != Convention.RowType.None)
          rowType0()
        }
      } else {
        rowType0()
      }
    }

    def rowType0(): RowType
  }
}
```

---

## ConventionFunc.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/ConventionFunc.scala`

```scala
package org.apache.gluten.extension.columnar.transition

/** ConventionFunc is a utility to derive [[Convention]] or [[ConventionReq]] from a query plan. */
sealed trait ConventionFunc {
  def conventionOf(plan: SparkPlan): Convention
  def conventionReqOf(plan: SparkPlan): Seq[ConventionReq]
}

object ConventionFunc {
  trait Override {
    def rowTypeOf: PartialFunction[SparkPlan, Convention.RowType] = PartialFunction.empty
    def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = PartialFunction.empty
    def conventionReqOf: PartialFunction[SparkPlan, Seq[ConventionReq]] = PartialFunction.empty
  }

  object Override {
    object Empty extends Override
  }

  def create(): ConventionFunc = {
    val batchOverride = newOverride()
    new BuiltinFunc(batchOverride)
  }

  private def newOverride(): Override = {
    // Components should override Backend's convention function. Hence, reversed injection order
    // is applied.
    val overrides = Component.sorted().reverse.map(_.convFuncOverride())
    if (overrides.isEmpty) {
      return Override.Empty
    }
    new Override {
      override val rowTypeOf: PartialFunction[SparkPlan, Convention.RowType] = {
        overrides.map(_.rowTypeOf).reduce((l, r) => l.orElse(r))
      }
      override val batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
        overrides.map(_.batchTypeOf).reduce((l, r) => l.orElse(r))
      }
      override val conventionReqOf: PartialFunction[SparkPlan, Seq[ConventionReq]] = {
        overrides.map(_.conventionReqOf).reduce((l, r) => l.orElse(r))
      }
    }
  }

  private class BuiltinFunc(o: Override) extends ConventionFunc {
    override def conventionOf(plan: SparkPlan): Convention = {
      val conv = conventionOf0(plan)
      conv
    }

    private def conventionOf0(plan: SparkPlan): Convention = plan match {
      case p if canPropagateConvention(p) =>
        val childrenConventions = p.children.map(conventionOf0).distinct
        if (childrenConventions.size > 1) {
          childrenConventions.reduce(_ && _)
        } else {
          assert(childrenConventions.size == 1)
          childrenConventions.head
        }
      case q: QueryStageExec => conventionOf0(q.plan)
      case r: ReusedExchangeExec => conventionOf0(r.child)
      case other =>
        val conv = Convention.of(rowTypeOf(other), batchTypeOf(other))
        conv
    }

    private def rowTypeOf(plan: SparkPlan): Convention.RowType = {
      val out = o.rowTypeOf.applyOrElse(plan, rowTypeOf0)
      out
    }

    private def rowTypeOf0(plan: SparkPlan): Convention.RowType = {
      val out = plan match {
        case k: Convention.KnownRowType =>
          k.rowType()
        case _ if SparkPlanUtil.supportsRowBased(plan) =>
          Convention.RowType.VanillaRowType
        case _ =>
          Convention.RowType.None
      }
      checkRowType(plan, out)
      out
    }

    private def checkRowType(plan: SparkPlan, rowType: Convention.RowType): Unit = {
      if (SparkPlanUtil.supportsRowBased(plan)) {
        assert(
          rowType != Convention.RowType.None,
          s"Plan ${plan.nodeName} supports row-based execution, " +
            s"however #rowTypeOf returns None")
      } else {
        assert(
          rowType == Convention.RowType.None,
          s"Plan ${plan.nodeName} doesn't support row-based " +
            s"execution, however #rowTypeOf returns $rowType")
      }
    }

    private def batchTypeOf(plan: SparkPlan): Convention.BatchType = {
      val out = o.batchTypeOf.applyOrElse(plan, batchTypeOf0)
      out
    }

    private def batchTypeOf0(plan: SparkPlan): Convention.BatchType = {
      val out = plan match {
        case k: Convention.KnownBatchType =>
          k.batchType()
        case _ if plan.supportsColumnar =>
          Convention.BatchType.VanillaBatchType
        case _ =>
          Convention.BatchType.None
      }
      checkBatchType(plan, out)
      out
    }

    private def checkBatchType(plan: SparkPlan, batchType: Convention.BatchType): Unit = {
      if (plan.supportsColumnar) {
        assert(
          batchType != Convention.BatchType.None,
          s"Plan ${plan.nodeName} supports columnar " +
            s"execution, however #batchTypeOf returns None")
      } else {
        assert(
          batchType == Convention.BatchType.None,
          s"Plan ${plan.nodeName} doesn't support " +
            s"columnar execution, however #batchTypeOf returns $batchType")
      }
    }

    override def conventionReqOf(plan: SparkPlan): Seq[ConventionReq] = {
      val req = o.conventionReqOf.applyOrElse(plan, conventionReqOf0)
      assert(req.size == plan.children.size)
      req
    }

    private def conventionReqOf0(plan: SparkPlan): Seq[ConventionReq] = plan match {
      case k: KnownChildConvention =>
        val reqs = k.requiredChildConvention()
        reqs
      case RowToColumnarLike(_) =>
        Seq(
          ConventionReq.of(
            ConventionReq.RowType.Is(Convention.RowType.VanillaRowType),
            ConventionReq.BatchType.Any))
      case ColumnarToRowExec(_) =>
        Seq(
          ConventionReq.of(
            ConventionReq.RowType.Any,
            ConventionReq.BatchType.Is(Convention.BatchType.VanillaBatchType)))
      case write: DataWritingCommandExec if SparkPlanUtil.isPlannedV1Write(write) =>
        // To align with ApplyColumnarRulesAndInsertTransitions#insertTransitions
        Seq(ConventionReq.any)
      case u: UnionExec =>
        // We force vanilla union to output row data to get the best compatibility with vanilla
        // Spark.
        // As a result it's a common practice to rewrite it with GlutenPlan for offloading.
        Seq.tabulate(u.children.size)(
          _ =>
            ConventionReq.of(
              ConventionReq.RowType.Is(Convention.RowType.VanillaRowType),
              ConventionReq.BatchType.Any))
      case other =>
        // In the normal case, children's convention should follow parent node's convention.
        val childReq = conventionOf0(other).asReq()
        Seq.tabulate(other.children.size)(
          _ => {
            childReq
          })
    }
  }
}
```

---

## ConventionReq.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/ConventionReq.scala`

```scala
package org.apache.gluten.extension.columnar.transition

/**
 * ConventionReq describes the requirement for [[Convention]]. This is mostly used in determining
 * the acceptable conventions for its children of a parent plan node.
 */
sealed trait ConventionReq {
  def requiredRowType: ConventionReq.RowType
  def requiredBatchType: ConventionReq.BatchType
}

object ConventionReq {
  sealed trait RowType

  object RowType {
    final case object Any extends RowType
    final case class Is(t: Convention.RowType) extends RowType {
      assert(t != Convention.RowType.None)
    }
  }

  sealed trait BatchType

  object BatchType {
    final case object Any extends BatchType
    final case class Is(t: Convention.BatchType) extends BatchType {
      assert(t != Convention.BatchType.None)
    }
  }

  private case class Impl(
      override val requiredRowType: RowType,
      override val requiredBatchType: BatchType
  ) extends ConventionReq

  val any: ConventionReq = of(RowType.Any, BatchType.Any)
  val vanillaRow: ConventionReq = ofRow(RowType.Is(Convention.RowType.VanillaRowType))
  val vanillaBatch: ConventionReq = ofBatch(BatchType.Is(Convention.BatchType.VanillaBatchType))

  def get(plan: SparkPlan): Seq[ConventionReq] = ConventionFunc.create().conventionReqOf(plan)
  def of(rowType: RowType, batchType: BatchType): ConventionReq = Impl(rowType, batchType)
  def ofRow(rowType: RowType): ConventionReq = Impl(rowType, BatchType.Any)
  def ofBatch(batchType: BatchType): ConventionReq = Impl(RowType.Any, batchType)

  trait KnownChildConvention {
    def requiredChildConvention(): Seq[ConventionReq]
  }
}
```

---

## FloydWarshallGraph.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/FloydWarshallGraph.scala`

```scala
package org.apache.gluten.extension.columnar.transition

/**
 * Floyd-Warshall algorithm for finding e.g., cheapest transition between query plan nodes.
 *
 * https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm
 */
trait FloydWarshallGraph[V <: AnyRef, E <: AnyRef] {
  def hasPath(from: V, to: V): Boolean
  def pathOf(from: V, to: V): Path[E]
}

object FloydWarshallGraph {
  trait Cost

  trait CostModel[E <: AnyRef] {
    def zero(): Cost
    def sum(one: Cost, other: Cost): Cost
    def costOf(edge: E): Cost
    def costComparator(): Ordering[Cost]
  }

  trait Path[E <: AnyRef] {
    def edges(): Seq[E]
    def cost(costModel: CostModel[E]): Cost
  }

  def builder[V <: AnyRef, E <: AnyRef](): Builder[V, E] = {
    Builder.create()
  }

  private object Path {
    def apply[E <: AnyRef](edges: Seq[E]): Path[E] = Impl(edges)
    private case class Impl[E <: AnyRef](override val edges: Seq[E]) extends Path[E] {
      override def cost(costModel: CostModel[E]): Cost = {
        edges
          .map(costModel.costOf)
          .reduceOption((c1, c2) => costModel.sum(c1, c2))
          .getOrElse(costModel.zero())
      }
    }
  }

  private class Impl[V <: AnyRef, E <: AnyRef](pathTable: Map[V, Map[V, Path[E]]])
    extends FloydWarshallGraph[V, E] {
    override def hasPath(from: V, to: V): Boolean = {
      if (!pathTable.contains(from)) {
        return false
      }
      val vec = pathTable(from)
      if (!vec.contains(to)) {
        return false
      }
      true
    }

    override def pathOf(from: V, to: V): Path[E] = {
      assert(hasPath(from, to))
      val path = pathTable(from)(to)
      path
    }
  }

  trait Builder[V <: AnyRef, E <: AnyRef] {
    def addVertex(v: V): Builder[V, E]
    def addEdge(from: V, to: V, edge: E): Builder[V, E]
    def build(costModel: CostModel[E]): FloydWarshallGraph[V, E]
  }

  private object Builder {
    private class Impl[V <: AnyRef, E <: AnyRef]() extends Builder[V, E] {
      private val pathTable: mutable.Map[V, mutable.Map[V, Path[E]]] = mutable.Map()
      private var graph: Option[FloydWarshallGraph[V, E]] = None

      override def addVertex(v: V): Builder[V, E] = {
        assert(!pathTable.contains(v), s"Vertex $v already exists in graph")
        pathTable.getOrElseUpdate(v, mutable.Map()).getOrElseUpdate(v, Path(Nil))
        graph = None
        this
      }

      override def addEdge(from: V, to: V, edge: E): Builder[V, E] = {
        assert(from != to, s"Input vertices $from and $to should be different")
        assert(pathTable.contains(from), s"Vertex $from not exists in graph")
        assert(pathTable.contains(to), s"Vertex $to not exists in graph")
        assert(!hasPath(from, to), s"Path from $from to $to already exists in graph")
        pathTable(from) += to -> Path(Seq(edge))
        graph = None
        this
      }

      override def build(costModel: CostModel[E]): FloydWarshallGraph[V, E] = {
        val vertices = pathTable.keys
        for (k <- vertices) {
          for (i <- vertices) {
            for (j <- vertices) {
              if (hasPath(i, k) && hasPath(k, j)) {
                val pathIk = pathTable(i)(k)
                val pathKj = pathTable(k)(j)
                val newPath = Path(pathIk.edges() ++ pathKj.edges())
                if (!hasPath(i, j)) {
                  pathTable(i) += j -> newPath
                } else {
                  val path = pathTable(i)(j)
                  if (
                    costModel
                      .costComparator()
                      .compare(newPath.cost(costModel), path.cost(costModel)) < 0
                  ) {
                    pathTable(i) += j -> newPath
                  }
                }
              }
            }
          }
        }
        new FloydWarshallGraph.Impl(pathTable.map { case (k, m) => (k, m.toMap) }.toMap)
      }

      private def hasPath(from: V, to: V): Boolean = {
        if (!pathTable.contains(from)) {
          return false
        }
        val vec = pathTable(from)
        if (!vec.contains(to)) {
          return false
        }
        true
      }
    }

    def create[V <: AnyRef, E <: AnyRef](): Builder[V, E] = {
      new Impl()
    }
  }
}
```

---

## Transition.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Transition.scala`

```scala
package org.apache.gluten.extension.columnar.transition

/**
 * Transition is a simple function to convert a query plan to interested [[ConventionReq]].
 *
 * Transitions can be registered through the utility APIs in
 * [[org.apache.gluten.extension.columnar.transition.Convention.BatchType]]'s definition.
 */
trait Transition {
  final def apply(plan: SparkPlan): SparkPlan = {
    val out = apply0(plan)
    out.copyTagsFrom(plan)
    out
  }

  final lazy val isEmpty: Boolean = {
    // Tests if a transition is actually no-op.
    val plan = DummySparkPlan()
    val out = apply0(plan)
    val identical = out eq plan
    identical
  }

  protected def apply0(plan: SparkPlan): SparkPlan
}

object Transition {
  val empty: Transition = (plan: SparkPlan) => plan
  private val abort: Transition = (_: SparkPlan) => throw new UnsupportedOperationException("Abort")
  val factory = Factory.newBuiltin()

  def notFound(plan: SparkPlan): GlutenException = {
    new GlutenException(s"No viable transition found from plan's child to itself: $plan")
  }

  def notFound(plan: SparkPlan, required: ConventionReq): GlutenException = {
    new GlutenException(s"No viable transition to [$required] found for plan: $plan")
  }

  trait Factory {
    final def findTransition(
        from: Convention,
        to: ConventionReq,
        otherwise: Exception): Transition = {
      findTransition(from, to) {
        throw otherwise
      }
    }

    final def satisfies(conv: Convention, req: ConventionReq): Boolean = {
      val transition = findTransition(conv, req)(abort)
      transition.isEmpty
    }

    def update(body: TransitionGraph.Builder => Unit): Unit

    protected[Factory] def findTransition(from: Convention, to: ConventionReq)(
        orElse: => Transition): Transition
  }

  private object Factory {
    def newBuiltin(): Factory = {
      new BuiltinFactory()
    }

    private class BuiltinFactory() extends Factory {
      private val graphBuilder: TransitionGraph.Builder = TransitionGraph.builder()
      // Use of this cache allows user to set a new cost model in the same Spark session,
      // then the new cost model will take effect for new transition-finding requests.
      private val graphCache = mutable.Map[String, TransitionGraph]()

      private def graph(): TransitionGraph = synchronized {
        val aliasOrClass = GlutenCoreConfig.get.rasCostModel
        graphCache.getOrElseUpdate(
          aliasOrClass, {
            val base = GlutenCostModel.find(aliasOrClass)
            graphBuilder.build(TransitionGraph.asTransitionCostModel(base))
          })
      }

      override def findTransition(from: Convention, to: ConventionReq)(
          orElse: => Transition): Transition = {
        assert(
          !from.isNone,
          "#findTransition called with on a plan that doesn't support either row or columnar " +
            "output")
        val out = (to.requiredRowType, to.requiredBatchType) match {
          case (ConventionReq.RowType.Is(toRowType), ConventionReq.BatchType.Is(toBatchType)) =>
            if (from.rowType == toRowType && from.batchType == toBatchType) {
              return Transition.empty
            } else {
              throw new UnsupportedOperationException(
                "Transiting to plan that both have row and columnar-batch output is not yet " +
                  "supported")
            }
          case (ConventionReq.RowType.Is(toRowType), ConventionReq.BatchType.Any) =>
            from.rowType match {
              case Convention.RowType.None =>
                // Input query plan doesn't have recognizable row-based output,
                // find columnar-to-row transition.
                graph().transitionOfOption(from.batchType, toRowType).getOrElse(orElse)
              case fromRowType if toRowType == fromRowType =>
                // We have only one single built-in row type.
                Transition.empty
              case _ =>
                // Find row-to-row transition.
                graph().transitionOfOption(from.rowType, toRowType).getOrElse(orElse)
            }
          case (ConventionReq.RowType.Any, ConventionReq.BatchType.Is(toBatchType)) =>
            from.batchType match {
              case Convention.BatchType.None =>
                // Input query plan doesn't have recognizable columnar output,
                // find row-to-columnar transition.
                graph().transitionOfOption(from.rowType, toBatchType).getOrElse(orElse)
              case fromBatchType if toBatchType == fromBatchType =>
                Transition.empty
              case fromBatchType =>
                // Find columnar-to-columnar transition.
                graph().transitionOfOption(fromBatchType, toBatchType).getOrElse(orElse)
            }
          case (ConventionReq.RowType.Any, ConventionReq.BatchType.Any) =>
            Transition.empty
          case _ =>
            throw new UnsupportedOperationException(
              s"Illegal convention requirement: $ConventionReq")
        }
        out
      }

      override def update(func: TransitionGraph.Builder => Unit): Unit = synchronized {
        func(graphBuilder)
        graphCache.clear()
      }
    }
  }
}
```

---

## TransitionGraph.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/TransitionGraph.scala`

```scala
package org.apache.gluten.extension.columnar.transition

object TransitionGraph {
  trait Vertex {
    private val initialized: AtomicBoolean = new AtomicBoolean(false)

    final def ensureRegistered(): Unit = {
      if (!initialized.compareAndSet(false, true)) {
        // Already registered.
        return
      }
      register()
    }

    final private def register(): Unit = BatchType.synchronized {
      Transition.factory.update(graph => graph.addVertex(this))
      register0()
    }

    protected[this] def register0(): Unit

    override def toString: String = SparkReflectionUtil.getSimpleClassName(this.getClass)
  }

  type Builder = FloydWarshallGraph.Builder[TransitionGraph.Vertex, Transition]

  private[transition] def builder(): Builder = {
    FloydWarshallGraph.builder()
  }

  private[transition] def asTransitionCostModel(
      base: GlutenCostModel): FloydWarshallGraph.CostModel[Transition] = {
    new TransitionCostModel(base)
  }

  implicit class TransitionGraphOps(val graph: TransitionGraph) {
    def hasTransition(from: TransitionGraph.Vertex, to: TransitionGraph.Vertex): Boolean = {
      graph.hasPath(from, to)
    }

    def transitionOf(from: TransitionGraph.Vertex, to: TransitionGraph.Vertex): Transition = {
      val path = graph.pathOf(from, to)
      val out = path.edges().reduceOption((l, r) => chain(l, r)).getOrElse(Transition.empty)
      out
    }

    def transitionOfOption(
        from: TransitionGraph.Vertex,
        to: TransitionGraph.Vertex): Option[Transition] = {
      if (!hasTransition(from, to)) {
        return None
      }
      Some(transitionOf(from, to))
    }
  }

  private case class ChainedTransition(first: Transition, second: Transition) extends Transition {
    override def apply0(plan: SparkPlan): SparkPlan = {
      second(first(plan))
    }
  }

  private object TransitionGraphOps {
    private def chain(first: Transition, second: Transition): Transition = {
      if (first.isEmpty && second.isEmpty) {
        return Transition.empty
      }
      ChainedTransition(first, second)
    }
  }

  /** Reuse RAS cost to represent transition cost. */
  private case class TransitionCost(value: GlutenCost, nodeNames: Seq[String])
    extends FloydWarshallGraph.Cost

  /**
   * The transition cost model relies on the registered Gluten cost model internally to evaluate
   * cost of transitions.
   *
   * Note the transition graph is built once for all subsequent Spark sessions created on the same
   * driver, so any access to Spark dynamic SQL config in Gluten cost model will not take effect for
   * the transition cost evaluation. Hence, it's not recommended to access Spark dynamic
   * configurations in Gluten cost model as well.
   */
  private class TransitionCostModel(base: GlutenCostModel)
    extends FloydWarshallGraph.CostModel[Transition] {

    override def zero(): TransitionCost = TransitionCost(base.makeZeroCost(), Nil)
    override def costOf(transition: Transition): TransitionCost = {
      costOf0(transition)
    }
    override def sum(
        one: FloydWarshallGraph.Cost,
        other: FloydWarshallGraph.Cost): FloydWarshallGraph.Cost = (one, other) match {
      case (TransitionCost(c1, p1), TransitionCost(c2, p2)) =>
        TransitionCost(base.sum(c1, c2), p1 ++ p2)
    }
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = {
      (x: FloydWarshallGraph.Cost, y: FloydWarshallGraph.Cost) =>
        (x, y) match {
          case (TransitionCost(v1, nodeNames1), TransitionCost(v2, nodeNames2)) =>
            val diff = base.costComparator().compare(v1, v2)
            if (diff != 0) {
              diff
            } else {
              // To make the output order stable.
              nodeNames1.mkString.hashCode - nodeNames2.mkString.hashCode
            }
        }
    }

    private def costOf0(transition: Transition): TransitionCost = {
      val leaf = DummySparkPlan()
      val transited = transition.apply(leaf)

      /**
       * The calculation considers C2C's cost as half of C2R / R2C's cost. So query planner prefers
       * C2C than C2R / R2C.
       */
      def rasCostOfPlan(plan: SparkPlan): GlutenCost = base.costOf(plan)
      def nodeNamesOfPlan(plan: SparkPlan): Seq[String] = {
        plan.map(_.nodeName).reverse
      }

      val leafCost = rasCostOfPlan(leaf)
      val accumulatedCost = rasCostOfPlan(transited)
      val costDiff = base.diff(accumulatedCost, leafCost)

      val leafNodeNames = nodeNamesOfPlan(leaf)
      val accumulatedNodeNames = nodeNamesOfPlan(transited)
      require(
        accumulatedNodeNames.startsWith(leafNodeNames),
        s"Transition should only add unary nodes on the input plan or leave it unchanged. " +
          s"Before: $leaf, after: $transited"
      )
      val nodeNamesDiff = mutable.ListBuffer[String]()
      nodeNamesDiff ++= accumulatedNodeNames
      leafNodeNames.foreach(n => assert(nodeNamesDiff.remove(0) == n))
      assert(
        nodeNamesDiff.size == accumulatedNodeNames.size - leafNodeNames.size,
        s"Dummy leaf node not found in the transited plan: $transited")

      TransitionCost(costDiff, nodeNamesDiff.toSeq)
    }
  }
}
```

---

## Transitions.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/Transitions.scala`

```scala
package org.apache.gluten.extension.columnar.transition

case class InsertTransitions(convReq: ConventionReq) extends Rule[SparkPlan] {
  private val convFunc = ConventionFunc.create()

  override def apply(plan: SparkPlan): SparkPlan = {
    // Remove all transitions at first.
    val removed = RemoveTransitions.apply(plan)
    val filled = fillWithTransitions(removed)
    val out = Transitions.enforceReq(filled, convReq)
    out
  }

  private def fillWithTransitions(plan: SparkPlan): SparkPlan = plan.transformUp {
    case node if node.children.nonEmpty => applyForNode(node)
  }

  private def applyForNode(node: SparkPlan): SparkPlan = {
    val convReqs = convFunc.conventionReqOf(node)
    val newChildren = node.children.zip(convReqs).map {
      case (child, convReq) =>
        val from = convFunc.conventionOf(child)
        if (from.isNone) {
          // For example, a union op with row child and columnar child at the same time,
          // The plan is actually not executable, and we cannot tell about its convention.
          child
        } else {
          val transition =
            Transition.factory.findTransition(from, convReq, Transition.notFound(node))
          val newChild = transition.apply(child)
          newChild
        }
    }
    node.withNewChildren(newChildren)
  }
}

object InsertTransitions {
  def create(outputsColumnar: Boolean, batchType: BatchType): InsertTransitions = {
    val conventionReq = if (outputsColumnar) {
      ConventionReq.ofBatch(ConventionReq.BatchType.Is(batchType))
    } else {
      ConventionReq.vanillaRow
    }
    InsertTransitions(conventionReq)
  }
}

object RemoveTransitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown { case p => removeForNode(p) }

  @tailrec
  private[transition] def removeForNode(plan: SparkPlan): SparkPlan = plan match {
    case ColumnarToRowLike(child) => removeForNode(child)
    case RowToColumnarLike(child) => removeForNode(child)
    case ColumnarToColumnarLike(child) => removeForNode(child)
    case other => other
  }
}

object Transitions {
  def insert(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    InsertTransitions.create(outputsColumnar, BatchType.VanillaBatchType).apply(plan)
  }

  def toRowPlan(plan: SparkPlan): SparkPlan = {
    enforceReq(plan, ConventionReq.vanillaRow)
  }

  def toBatchPlan(plan: SparkPlan, toBatchType: Convention.BatchType): SparkPlan = {
    enforceReq(plan, ConventionReq.ofBatch(ConventionReq.BatchType.Is(toBatchType)))
  }

  def enforceReq(plan: SparkPlan, req: ConventionReq): SparkPlan = {
    val convFunc = ConventionFunc.create()
    val removed = RemoveTransitions.removeForNode(plan)
    val transition = Transition.factory
      .findTransition(convFunc.conventionOf(removed), req, Transition.notFound(removed, req))
    val out = transition.apply(removed)
    out
  }
}
```

---

## package.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/transition/package.scala`

```scala
package org.apache.gluten.extension.columnar

package object transition {

  type TransitionGraph = FloydWarshallGraph[TransitionGraph.Vertex, Transition]
  // These 5 plan operators (as of Spark 3.5) are operators that have the
  // same convention with their children.
  //
  // Extend this list in shim layer once Spark has more.
  def canPropagateConvention(plan: SparkPlan): Boolean = plan match {
    case p: DebugExec => true
    case p: UnionExec if SparkVersionUtil.gteSpark33 =>
      true
    case p: AQEShuffleReadExec => true
    case p: InputAdapter => true
    case p: WholeStageCodegenExec => true
    case _ => false
  }

  // Extractor for Spark/Gluten's C2R
  object ColumnarToRowLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case c2r: ColumnarToRowTransition =>
          Some(c2r.child)
        case _ => None
      }
    }
  }

  // Extractor for Spark/Gluten's R2C
  object RowToColumnarLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case r2c: RowToColumnarTransition =>
          Some(r2c.child)
        case _ => None
      }
    }
  }

  // Extractor for Gluten's C2C with different convention
  object ColumnarToColumnarLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case c2c: ColumnarToColumnarTransition if !c2c.isSameConvention =>
          Some(c2c.child)
        case _ => None
      }
    }
  }

  case class DummySparkPlan() extends LeafExecNode {
    override def supportsColumnar: Boolean = true // To bypass the assertion in ColumnarToRowExec.
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = Nil
  }
}
```

---

## FloydWarshallGraphSuite.scala

**Path**: `../incubator-gluten/gluten-core/src/test/scala/org/apache/gluten/extension/columnar/transition/FloydWarshallGraphSuite.scala`

```scala
package org.apache.gluten.extension.columnar.transition

class FloydWarshallGraphSuite extends AnyFunSuite {
  test("Sanity") {
    val v0 = Vertex()
    val v1 = Vertex()
    val v2 = Vertex()
    val v3 = Vertex()
    val v4 = Vertex()

    val e01 = Edge(5)
    val e12 = Edge(6)
    val e03 = Edge(2)
    val e34 = Edge(1)
    val e42 = Edge(3)

    val graph = FloydWarshallGraph
      .builder()
      .addVertex(v0)
      .addVertex(v1)
      .addVertex(v2)
      .addVertex(v3)
      .addVertex(v4)
      .addEdge(v0, v1, e01)
      .addEdge(v1, v2, e12)
      .addEdge(v0, v3, e03)
      .addEdge(v3, v4, e34)
      .addEdge(v4, v2, e42)
      .build(CostModel)

    assert(graph.hasPath(v0, v1))
    assert(graph.hasPath(v0, v2))
    assert(!graph.hasPath(v1, v0))
    assert(!graph.hasPath(v2, v0))

    assert(graph.pathOf(v0, v0).edges() == Nil)

    assert(graph.pathOf(v0, v1).edges() == Seq(e01))
    assert(graph.pathOf(v1, v2).edges() == Seq(e12))
    assert(graph.pathOf(v0, v3).edges() == Seq(e03))
    assert(graph.pathOf(v3, v4).edges() == Seq(e34))
    assert(graph.pathOf(v4, v2).edges() == Seq(e42))

    assert(graph.pathOf(v0, v2).edges() == Seq(e03, e34, e42))
  }
}

private object FloydWarshallGraphSuite {
  case class Vertex private (id: Int)

  private object Vertex {
    private val id = new AtomicInteger(0)

    def apply(): Vertex = {
      Vertex(id.getAndIncrement())
    }
  }

  case class Edge private (id: Int, distance: Long)

  private object Edge {
    private val id = new AtomicInteger(0)

    def apply(distance: Long): Edge = {
      Edge(id.getAndIncrement(), distance)
    }
  }

  private case class LongCost(c: Long) extends FloydWarshallGraph.Cost

  private object CostModel extends FloydWarshallGraph.CostModel[Edge] {
    override def zero(): FloydWarshallGraph.Cost = LongCost(0)
    override def sum(
        one: FloydWarshallGraph.Cost,
        other: FloydWarshallGraph.Cost): FloydWarshallGraph.Cost = {
      LongCost(one.asInstanceOf[LongCost].c + other.asInstanceOf[LongCost].c)
    }
    override def costOf(edge: Edge): FloydWarshallGraph.Cost = LongCost(edge.distance * 10)
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = Ordering.Long.on {
      case LongCost(c) => c
    }
  }
}
```

---

# util

*This section contains 10 source files.*

## SparkThreadPoolUtil.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/spark/util/SparkThreadPoolUtil.java`

```java
package org.apache.spark.util;

public class SparkThreadPoolUtil {
  private static final ExecutorService GC_THREAD_POOL =
      ThreadUtils.newDaemonSingleThreadExecutor("gc-thread-pool");

  private static final AtomicBoolean PERFORMING_GC = new AtomicBoolean(false);

  public static void triggerGCInThreadPool(Runnable callback) {
    if (PERFORMING_GC.compareAndSet(false, true)) {
      GC_THREAD_POOL.submit(
          new Runnable() {
            @Override
            public void run() {
              try {
                callback.run();
              } finally {
                PERFORMING_GC.set(false);
              }
            }
          });
    }
  }
}
```

---

## SparkDirectoryUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkDirectoryUtil.scala`

```scala
package org.apache.spark.util

/**
 * Manages Gluten's local directories, for storing jars, libs, spill files, or other temporary
 * stuffs.
 */
class SparkDirectoryUtil private (val roots: Array[String]) extends Logging {
  private val ROOTS: Array[File] = roots.flatMap {
    rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "gluten")
        SparkShutdownManagerUtil.addHookForTempDirRemoval(
          () => {
            try FileUtils.forceDelete(localDir)
            catch {
              case e: Exception =>
                throw new GlutenException(e)
            }
          })
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create Gluten local dir in $rootDir. Ignoring this directory.", e)
          None
      }
  }

  private val NAMESPACE_MAPPING: java.util.Map[String, Namespace] =
    new ConcurrentHashMap[String, Namespace]

  def namespace(name: String): Namespace =
    NAMESPACE_MAPPING.computeIfAbsent(name, (name: String) => new Namespace(ROOTS, name))
}

object SparkDirectoryUtil extends Logging {
  @volatile private var roots: Array[String] = _
  private lazy val INSTANCE: SparkDirectoryUtil = {
    if (this.roots == null) {
      throw new IllegalStateException("SparkDirectoryUtil not initialized")
    }
    new SparkDirectoryUtil(this.roots)
  }

  def init(conf: SparkConf): Unit = {
    val roots = Utils.getConfiguredLocalDirs(conf)
    init(roots)
  }

  private def init(roots: Array[String]): Unit = synchronized {
    if (this.roots == null) {
      this.roots = roots
    } else if (this.roots.toSet != roots.toSet) {
      throw new IllegalArgumentException(
        s"Reinitialize SparkDirectoryUtil with different root dirs: old: ${this.roots
            .mkString("Array(", ", ", ")")}, new: ${roots.mkString("Array(", ", ", ")")}"
      )
    }
  }

  def get(): SparkDirectoryUtil = INSTANCE
}

class Namespace(private val parents: Array[File], private val name: String) {
  val all = parents.map {
    root =>
      val path = Paths
        .get(root.getAbsolutePath)
        .resolve(name)
      path.toFile
  }

  private val cycleLooper = Stream.continually(all).flatten.toIterator

  def mkChildDirRoundRobin(childDirName: String): File = synchronized {
    if (!cycleLooper.hasNext) {
      throw new IllegalStateException()
    }
    val subDir = cycleLooper.next()
    if (StringUtils.isEmpty(subDir.getAbsolutePath)) {
      throw new IllegalArgumentException(s"Illegal local dir: $subDir")
    }
    val path = Paths
      .get(subDir.getAbsolutePath)
      .resolve(childDirName)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file
  }

  def mkChildDirRandomly(childDirName: String): File = {
    val selected = all(scala.util.Random.nextInt(all.length))
    val path = Paths
      .get(selected.getAbsolutePath)
      .resolve(childDirName)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file
  }

  def mkChildDirs(childDirName: String): Array[File] = {
    all.map {
      subDir =>
        val path = Paths
          .get(subDir.getAbsolutePath)
          .resolve(childDirName)
        val file = path.toFile
        FileUtils.forceMkdir(file)
        file
    }
  }
}
```

---

## SparkPlanUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkPlanUtil.scala`

```scala
package org.apache.spark.util

object SparkPlanUtil {

  def supportsRowBased(plan: SparkPlan): Boolean = {
    if (SparkVersionUtil.lteSpark32) {
      return !plan.supportsColumnar
    }

    val m = classOf[SparkPlan].getMethod("supportsRowBased")
    m.invoke(plan).asInstanceOf[Boolean]
  }

  def isPlannedV1Write(plan: DataWritingCommandExec): Boolean = {
    if (SparkVersionUtil.lteSpark33) {
      return false
    }

    val v1WriteCommandClass =
      Utils.classForName("org.apache.spark.sql.execution.datasources.V1WriteCommand")
    val plannedWriteEnabled =
      SQLConf.get.getConfString("spark.sql.optimizer.plannedWrite.enabled", "true").toBoolean
    v1WriteCommandClass.isAssignableFrom(plan.cmd.getClass) && plannedWriteEnabled
  }
}
```

---

## SparkReflectionUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkReflectionUtil.scala`

```scala
package org.apache.spark.util

object SparkReflectionUtil {
  def getSimpleClassName(cls: Class[_]): String = {
    Utils.getSimpleName(cls)
  }

  def classForName[C](
      className: String,
      initialize: Boolean = true,
      noSparkClassLoader: Boolean = false): Class[C] = {
    Utils.classForName(className, initialize, noSparkClassLoader)
  }
}
```

---

## SparkResourceUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkResourceUtil.scala`

```scala
package org.apache.spark.util

object SparkResourceUtil extends Logging {
  private val MEMORY_OVERHEAD_FACTOR = "spark.executor.memoryOverheadFactor"
  private val MIN_MEMORY_OVERHEAD = "spark.executor.minMemoryOverhead"

  /** Get the total cores of the Spark application */
  def getTotalCores(sqlConf: SQLConf): Int = {
    sqlConf.getConfString("spark.master") match {
      case local if local.startsWith("local") =>
        sqlConf.getConfString("spark.default.parallelism", "1").toInt
      case otherResourceManager if otherResourceManager.matches("(yarn|k8s:).*") =>
        val instances = getExecutorNum(sqlConf)
        val cores = sqlConf.getConfString("spark.executor.cores", "1").toInt
        Math.max(instances * cores, sqlConf.getConfString("spark.default.parallelism", "1").toInt)
      case standalone if standalone.startsWith("spark:") =>
        Math.max(
          sqlConf.getConfString("spark.cores.max", "1").toInt,
          sqlConf.getConfString("spark.default.parallelism", "1").toInt)
    }
  }

  /** Get the executor number for yarn */
  def getExecutorNum(sqlConf: SQLConf): Int = {
    if (sqlConf.getConfString("spark.dynamicAllocation.enabled", "false").toBoolean) {
      val maxExecutors =
        sqlConf
          .getConfString(
            "spark.dynamicAllocation.maxExecutors",
            sqlConf.getConfString("spark.default.parallelism", "1"))
          .toInt
      maxExecutors
    } else {
      sqlConf.getConfString("spark.executor.instances", "1").toInt
    }
  }

  def getExecutorCores(conf: SparkConf): Int = {
    val master = conf.get("spark.master")

    // part of the code originated from org.apache.spark.SparkContext#numDriverCores
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }

    val cores = master match {
      case "local" => 1
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case _ => conf.getInt("spark.executor.cores", 1)
    }

    cores
  }

  def getTaskSlots(conf: SparkConf): Int = {
    val executorCores = SparkResourceUtil.getExecutorCores(conf)
    val taskCores = conf.getInt("spark.task.cpus", 1)
    executorCores / taskCores
  }

  def isLocalMaster(conf: SparkConf): Boolean = {
    Utils.isLocalMaster(conf)
  }

  // Returns whether user manually sets memory overhead.
  def isMemoryOverheadSet(conf: SparkConf): Boolean = {
    Seq(EXECUTOR_MEMORY_OVERHEAD.key, MEMORY_OVERHEAD_FACTOR, MIN_MEMORY_OVERHEAD).exists(
      conf.contains)
  }

  def getMemoryOverheadSize(conf: SparkConf): Long = {
    val overheadMib = conf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse {
      val executorMemMib = conf.get(EXECUTOR_MEMORY)
      val factor =
        conf.getDouble(MEMORY_OVERHEAD_FACTOR, 0.1d)
      val minMib = conf.getLong(MIN_MEMORY_OVERHEAD, 384L)
      (executorMemMib * factor).toLong.max(minMib)
    }
    ByteUnit.MiB.toBytes(overheadMib)
  }
}
```

---

## SparkShutdownManagerUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkShutdownManagerUtil.scala`

```scala
package org.apache.spark.util

object SparkShutdownManagerUtil {
  def addHook(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  def addHookForLibUnloading(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY)(hook)
  }

  def addHookForTempDirRemoval(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY)(hook)
  }
}
```

---

## SparkTaskUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkTaskUtil.scala`

```scala
package org.apache.spark.util

object SparkTaskUtil {
  def setTaskContext(taskContext: TaskContext): Unit = {
    TaskContext.setTaskContext(taskContext)
  }

  def unsetTaskContext(): Unit = {
    TaskContext.unset()
  }

  def getTaskMemoryManager(taskContext: TaskContext): TaskMemoryManager = {
    taskContext.taskMemoryManager()
  }

  def createTestTaskContext(properties: Properties): TaskContext = {
    val conf = new SparkConf()
    conf.setAll(properties.asScala)
    val memoryManager = UnifiedMemoryManager(conf, 1)
    BlockManagerUtil.setTestMemoryStore(conf, memoryManager, isDriver = false)
    val stageId = -1.asInstanceOf[Object]
    val stageAttemptNumber = -1.asInstanceOf[Object]
    val partitionId = -1.asInstanceOf[Object]
    val taskAttemptId = -1L.asInstanceOf[Object]
    val attemptNumber = -1.asInstanceOf[Object]
    val numPartitions = -1.asInstanceOf[Object] // Added in Spark 3.4.
    val taskMemoryManager = new TaskMemoryManager(memoryManager, -1L).asInstanceOf[Object]
    val localProperties = properties.asInstanceOf[Object]
    val metricsSystem =
      MetricsSystem.createMetricsSystem("GLUTEN_UNSAFE", conf).asInstanceOf[Object]
    val taskMetrics = TaskMetrics.empty.asInstanceOf[Object]
    val cpus = 1.asInstanceOf[Object] // Added in Spark 3.3.
    val resources = Map.empty.asInstanceOf[Object]

    val ctor = {
      val ctors = classOf[TaskContextImpl].getDeclaredConstructors
      assert(ctors.size == 1)
      ctors.head
    }

    if (SparkVersionUtil.lteSpark32) {
      return ctor
        .newInstance(
          stageId,
          stageAttemptNumber,
          partitionId,
          taskAttemptId,
          attemptNumber,
          taskMemoryManager,
          localProperties,
          metricsSystem,
          taskMetrics,
          resources
        )
        .asInstanceOf[TaskContext]
    }

    if (SparkVersionUtil.eqSpark33) {
      return ctor
        .newInstance(
          stageId,
          stageAttemptNumber,
          partitionId,
          taskAttemptId,
          attemptNumber,
          taskMemoryManager,
          localProperties,
          metricsSystem,
          taskMetrics,
          cpus,
          resources
        )
        .asInstanceOf[TaskContext]
    }

    // Since Spark 3.4.
    ctor
      .newInstance(
        stageId,
        stageAttemptNumber,
        partitionId,
        taskAttemptId,
        attemptNumber,
        numPartitions,
        taskMemoryManager,
        localProperties,
        metricsSystem,
        taskMetrics,
        cpus,
        resources
      )
      .asInstanceOf[TaskContext]
  }
}
```

---

## SparkTestUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkTestUtil.scala`

```scala
package org.apache.spark.util

object SparkTestUtil {
  def isTesting: Boolean = {
    Utils.isTesting
  }
}
```

---

## SparkVersionUtil.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/spark/util/SparkVersionUtil.scala`

```scala
package org.apache.spark.util

object SparkVersionUtil {
  val lteSpark32: Boolean = compareMajorMinorVersion((3, 2)) <= 0
  private val comparedWithSpark33 = compareMajorMinorVersion((3, 3))
  private val comparedWithSpark35 = compareMajorMinorVersion((3, 5))
  val eqSpark33: Boolean = comparedWithSpark33 == 0
  val lteSpark33: Boolean = lteSpark32 || eqSpark33
  val gteSpark33: Boolean = comparedWithSpark33 >= 0
  val gteSpark35: Boolean = comparedWithSpark35 >= 0

  // Returns X. X < 0 if one < other, x == 0 if one == other, x > 0 if one > other.
  def compareMajorMinorVersion(other: (Int, Int)): Int = {
    val (major, minor) = VersionUtils.majorMinorVersion(org.apache.spark.SPARK_VERSION)
    if (major == other._1) {
      minor - other._2
    } else {
      major - other._1
    }
  }
}
```

---

## ResourceUtilTest.java

**Path**: `../incubator-gluten/gluten-core/src/test/java/org/apache/gluten/util/ResourceUtilTest.java`

```java
package org.apache.gluten.util;

public class ResourceUtilTest {
  @Test
  public void testFile() {
    // Use the class file of this test to verify the sanity of ResourceUtil.
    List<String> classes =
        ResourceUtil.getResources(
            "org", Pattern.compile("apache/gluten/util/ResourceUtilTest\\.class"));
    Assert.assertEquals(1, classes.size());
    Assert.assertEquals("apache/gluten/util/ResourceUtilTest.class", classes.get(0));
  }

  @Test
  public void testJar() {
    // Use the class file of Spark code to verify the sanity of ResourceUtil.
    List<String> classes =
        ResourceUtil.getResources("org", Pattern.compile("apache/spark/SparkContext\\.class"));
    Assert.assertEquals(1, classes.size());
    Assert.assertEquals("apache/spark/SparkContext.class", classes.get(0));
  }
}
```

---

# utils

*This section contains 2 source files.*

## ConfigUtil.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/utils/ConfigUtil.java`

```java
package org.apache.gluten.utils;

public class ConfigUtil {

  public static byte[] serialize(Map<String, String> conf) {
    ConfigMap.Builder builder = ConfigMap.newBuilder();
    builder.putAllConfigs(conf);
    return builder.build().toByteArray();
  }
}
```

---

## ResourceUtil.java

**Path**: `../incubator-gluten/gluten-core/src/main/java/org/apache/gluten/utils/ResourceUtil.java`

```java
package org.apache.gluten.utils;

/**
 * Code is copied from <a
 * href="https://stackoverflow.com/questions/3923129/get-a-list-of-resources-from-classpath-directory">here</a>
 * and then modified for Gluten's use.
 */
public class ResourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);

  /**
   * Get a collection of resource paths by the input RegEx pattern in a certain container folder.
   *
   * @param container The container folder. E.g., `META-INF`. Should not be left empty, because
   *     Classloader requires for at a meaningful file name to search inside the loaded jar files.
   * @param pattern The pattern to match on the file names.
   * @return The relative resource paths in the order they are found.
   */
  public static List<String> getResources(final String container, final Pattern pattern) {
    Preconditions.checkArgument(
        !container.isEmpty(),
        "Resource search should only be used under a certain container folder");
    Preconditions.checkArgument(
        !container.startsWith("/") && !container.endsWith("/"),
        "Resource container should not start or end with\"/\"");
    final List<String> buffer = new ArrayList<>();
    final Enumeration<URL> containerUrls;
    try {
      containerUrls = Thread.currentThread().getContextClassLoader().getResources(container);
    } catch (IOException e) {
      throw new GlutenException(e);
    }
    while (containerUrls.hasMoreElements()) {
      final URL containerUrl = containerUrls.nextElement();
      getResources(containerUrl, pattern, buffer);
    }
    return Collections.unmodifiableList(buffer);
  }

  private static void getResources(
      final URL containerUrl, final Pattern pattern, final List<String> buffer) {
    final String protocol = containerUrl.getProtocol();
    switch (protocol) {
      case "file":
        final File fileContainer = new File(containerUrl.getPath());
        Preconditions.checkState(
            fileContainer.exists() && fileContainer.isDirectory(),
            "Specified file container " + containerUrl + " is not a directory or not a file");
        getResourcesFromDirectory(fileContainer, fileContainer, pattern, buffer);
        break;
      case "jar":
        final String jarContainerPath = containerUrl.getPath();
        final Pattern jarContainerPattern = Pattern.compile("file:([^!]+)!/(.+)");
        final Matcher m = jarContainerPattern.matcher(jarContainerPath);
        if (!m.matches()) {
          throw new GlutenException("Illegal Jar container URL: " + containerUrl);
        }
        final String jarPath = m.group(1);
        final File jarFile = new File(jarPath);
        Preconditions.checkState(
            jarFile.exists() && jarFile.isFile(),
            "Specified Jar container " + containerUrl + " is not a Jar file");
        final String dir = m.group(2);
        getResourcesFromJarFile(jarFile, dir, pattern, buffer);
        break;
      default:
        throw new GlutenException("Unrecognizable resource protocol: " + protocol);
    }
  }

  private static void getResourcesFromJarFile(
      final File jarFile, final String dir, final Pattern pattern, final List<String> buffer) {
    final ZipFile zf;
    try {
      zf = new ZipFile(jarFile);
    } catch (final IOException e) {
      throw new GlutenException(e);
    }
    final Enumeration<? extends ZipEntry> e = zf.entries();
    while (e.hasMoreElements()) {
      final ZipEntry ze = e.nextElement();
      final String fileName = ze.getName();
      if (!fileName.startsWith(dir)) {
        continue;
      }
      final String relativeFileName =
          new File(dir).toURI().relativize(new File(fileName).toURI()).getPath();
      final boolean accept = pattern.matcher(relativeFileName).matches();
      if (accept) {
        buffer.add(relativeFileName);
      }
    }
    try {
      zf.close();
    } catch (final IOException e1) {
      throw new GlutenException(e1);
    }
  }

  private static void getResourcesFromDirectory(
      final File root, final File directory, final Pattern pattern, final List<String> buffer) {
    final File[] fileList = Objects.requireNonNull(directory.listFiles());
    for (final File file : fileList) {
      if (file.isDirectory()) {
        getResourcesFromDirectory(root, file, pattern, buffer);
      } else {
        final String relative = root.toURI().relativize(file.toURI()).getPath();
        final boolean accept = pattern.matcher(relative).matches();
        if (accept) {
          buffer.add(relative);
        }
      }
    }
  }
}
```

---

# validator

*This section contains 1 source files.*

## Validator.scala

**Path**: `../incubator-gluten/gluten-core/src/main/scala/org/apache/gluten/extension/columnar/validator/Validator.scala`

```scala
package org.apache.gluten.extension.columnar.validator

trait Validator {
  def validate(plan: SparkPlan): OutCome

  final def pass(): OutCome = {
    Passed
  }

  final def fail(p: SparkPlan): OutCome = {
    Validator.Failed(s"[${getClass.getSimpleName}] Validation failed on node ${p.nodeName}")
  }

  final def fail(reason: String): OutCome = {
    Validator.Failed(reason)
  }
}

object Validator {
  sealed trait OutCome
  case object Passed extends OutCome
  case class Failed private (reason: String) extends OutCome

  def builder(): Builder = Builder()

  class Builder private {
    private val buffer: ListBuffer[Validator] = mutable.ListBuffer()

    /** Add a custom validator to pipeline. */
    def add(validator: Validator): Builder = {
      buffer ++= flatten(validator)
      this
    }

    def build(): Validator = {
      if (buffer.isEmpty) {
        return NoopValidator
      }
      if (buffer.size == 1) {
        return buffer.head
      }
      new ValidatorPipeline(buffer.toSeq)
    }

    private def flatten(validator: Validator): Seq[Validator] = validator match {
      case p: ValidatorPipeline =>
        p.validators.flatMap(flatten)
      case other => Seq(other)
    }
  }

  private object Builder {
    def apply(): Builder = new Builder()

    private object NoopValidator extends Validator {
      override def validate(plan: SparkPlan): Validator.OutCome = pass()
    }

    private class ValidatorPipeline(val validators: Seq[Validator]) extends Validator {
      assert(!validators.exists(_.isInstanceOf[ValidatorPipeline]))

      override def validate(plan: SparkPlan): Validator.OutCome = {
        val init: Validator.OutCome = pass()
        val finalOut = validators.foldLeft(init) {
          case (out, validator) =>
            out match {
              case Validator.Passed => validator.validate(plan)
              case Validator.Failed(_) => out
            }
        }
        finalOut
      }
    }
  }

  implicit class ValidatorImplicits(v: Validator) {
    def andThen(other: Validator): Validator = {
      builder().add(v).add(other).build()
    }
  }
}
```

---



**Total files processed**: 128
