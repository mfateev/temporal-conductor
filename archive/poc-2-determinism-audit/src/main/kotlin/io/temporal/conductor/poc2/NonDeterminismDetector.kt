package io.temporal.conductor.poc2

import net.bytebuddy.agent.ByteBuddyAgent
import net.bytebuddy.agent.builder.AgentBuilder
import net.bytebuddy.asm.Advice
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.matcher.ElementMatchers
import java.time.Instant
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Represents a detected non-deterministic call
 */
data class NonDeterministicCall(
    val methodName: String,
    val className: String,
    val stackTrace: List<String>,
    val timestamp: Long = System.nanoTime()
) {
    val conductorFrames: List<String>
        get() = stackTrace.filter {
            it.contains("com.netflix.conductor") ||
            it.contains("io.temporal.conductor")
        }

    val category: Category
        get() = when {
            // IDGenerator uses UUID - critical for task IDs
            methodName.contains("IDGenerator") -> Category.BLOCKING
            conductorFrames.any { it.contains("IDGenerator") } -> Category.BLOCKING
            // Timeout checks use currentTimeMillis
            conductorFrames.any { it.contains("checkWorkflowTimeout") || it.contains("checkTaskTimeout") } -> Category.BLOCKING
            // Logging/monitoring - cosmetic
            conductorFrames.any { it.contains("Monitors") || it.contains("LOGGER") } -> Category.COSMETIC
            // Everything else needs evaluation
            else -> Category.NEEDS_EVALUATION
        }

    enum class Category {
        BLOCKING,      // Used in scheduling decisions - must fix
        COSMETIC,      // Used only for logging - can ignore
        CONTAINABLE,   // Can be mocked/wrapped
        NEEDS_EVALUATION
    }

    override fun toString(): String {
        return """
            |$methodName (${category})
            |  Conductor frames:
            |${conductorFrames.joinToString("\n") { "    $it" }}
        """.trimMargin()
    }
}

/**
 * Collector for non-deterministic calls detected during execution
 */
object NonDeterminismCollector {
    private val calls = CopyOnWriteArrayList<NonDeterministicCall>()
    private val uniqueCalls = ConcurrentHashMap<String, NonDeterministicCall>()
    @Volatile
    var enabled = false

    fun record(methodName: String, className: String) {
        if (!enabled) return

        val stackTrace = Thread.currentThread().stackTrace
            .drop(3) // Skip getStackTrace, record, and advice method
            .map { "${it.className}.${it.methodName}(${it.fileName}:${it.lineNumber})" }

        // Only record if called from Conductor code
        val hasConductorFrame = stackTrace.any {
            it.contains("com.netflix.conductor") ||
            it.contains("io.temporal.conductor")
        }
        if (!hasConductorFrame) return

        val call = NonDeterministicCall(methodName, className, stackTrace)
        calls.add(call)

        // Track unique calls by method + first conductor frame
        val key = "$methodName:${call.conductorFrames.firstOrNull() ?: "unknown"}"
        uniqueCalls.putIfAbsent(key, call)
    }

    fun getCalls(): List<NonDeterministicCall> = calls.toList()
    fun getUniqueCalls(): List<NonDeterministicCall> = uniqueCalls.values.toList()

    fun clear() {
        calls.clear()
        uniqueCalls.clear()
    }

    fun printReport() {
        println("\n" + "=".repeat(70))
        println("NON-DETERMINISM AUDIT REPORT")
        println("=".repeat(70))

        val uniqueList = getUniqueCalls()
        if (uniqueList.isEmpty()) {
            println("\n✅ No non-deterministic calls detected!")
            return
        }

        println("\nTotal calls detected: ${calls.size}")
        println("Unique call sites: ${uniqueList.size}")

        val grouped = uniqueList.groupBy { it.category }

        println("\n" + "-".repeat(70))
        println("BLOCKING (${grouped[NonDeterministicCall.Category.BLOCKING]?.size ?: 0})")
        println("-".repeat(70))
        grouped[NonDeterministicCall.Category.BLOCKING]?.forEach { println(it) }

        println("\n" + "-".repeat(70))
        println("COSMETIC (${grouped[NonDeterministicCall.Category.COSMETIC]?.size ?: 0})")
        println("-".repeat(70))
        grouped[NonDeterministicCall.Category.COSMETIC]?.forEach { println(it) }

        println("\n" + "-".repeat(70))
        println("NEEDS EVALUATION (${grouped[NonDeterministicCall.Category.NEEDS_EVALUATION]?.size ?: 0})")
        println("-".repeat(70))
        grouped[NonDeterministicCall.Category.NEEDS_EVALUATION]?.forEach { println(it) }

        println("\n" + "=".repeat(70))
    }
}

/**
 * ByteBuddy advice classes for intercepting non-deterministic methods
 */
object SystemTimeAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onCurrentTimeMillis() {
        NonDeterminismCollector.record("System.currentTimeMillis()", "java.lang.System")
    }
}

object SystemNanoTimeAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onNanoTime() {
        NonDeterminismCollector.record("System.nanoTime()", "java.lang.System")
    }
}

object UUIDAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onRandomUUID() {
        NonDeterminismCollector.record("UUID.randomUUID()", "java.util.UUID")
    }
}

object RandomAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onNext() {
        NonDeterminismCollector.record("Random.next*()", "java.util.Random")
    }
}

object MathRandomAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onRandom() {
        NonDeterminismCollector.record("Math.random()", "java.lang.Math")
    }
}

object DateAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onDateInit() {
        NonDeterminismCollector.record("new Date()", "java.util.Date")
    }
}

object InstantNowAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onNow() {
        NonDeterminismCollector.record("Instant.now()", "java.time.Instant")
    }
}

object LocalDateTimeNowAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onNow() {
        NonDeterminismCollector.record("LocalDateTime.now()", "java.time.LocalDateTime")
    }
}

object IDGeneratorAdvice {
    @JvmStatic
    @Advice.OnMethodEnter
    fun onGenerate() {
        NonDeterminismCollector.record("IDGenerator.generate()", "com.netflix.conductor.core.utils.IDGenerator")
    }
}

/**
 * Installs ByteBuddy instrumentation to detect non-deterministic calls
 */
object NonDeterminismDetector {

    fun install() {
        println("Installing non-determinism detection instrumentation...")

        // Install ByteBuddy agent
        ByteBuddyAgent.install()

        // Build and install transformers
        AgentBuilder.Default()
            .disableClassFormatChanges()
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
            .type(ElementMatchers.named("java.lang.System"))
            .transform { builder, _, _, _, _ ->
                builder
                    .visit(Advice.to(SystemTimeAdvice::class.java)
                        .on(ElementMatchers.named<MethodDescription>("currentTimeMillis")))
                    .visit(Advice.to(SystemNanoTimeAdvice::class.java)
                        .on(ElementMatchers.named<MethodDescription>("nanoTime")))
            }
            .type(ElementMatchers.named("java.util.UUID"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(UUIDAdvice::class.java)
                    .on(ElementMatchers.named<MethodDescription>("randomUUID")))
            }
            .type(ElementMatchers.named("java.util.Random"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(RandomAdvice::class.java)
                    .on(ElementMatchers.nameStartsWith<MethodDescription>("next")))
            }
            .type(ElementMatchers.named("java.lang.Math"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(MathRandomAdvice::class.java)
                    .on(ElementMatchers.named<MethodDescription>("random")))
            }
            .type(ElementMatchers.named("java.util.Date"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(DateAdvice::class.java)
                    .on(ElementMatchers.isConstructor<MethodDescription>().and(ElementMatchers.takesArguments<MethodDescription>(0))))
            }
            .type(ElementMatchers.named("java.time.Instant"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(InstantNowAdvice::class.java)
                    .on(ElementMatchers.named<MethodDescription>("now").and(ElementMatchers.takesArguments<MethodDescription>(0))))
            }
            .type(ElementMatchers.named("java.time.LocalDateTime"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(LocalDateTimeNowAdvice::class.java)
                    .on(ElementMatchers.named<MethodDescription>("now").and(ElementMatchers.takesArguments<MethodDescription>(0))))
            }
            // Conductor-specific: IDGenerator
            .type(ElementMatchers.named("com.netflix.conductor.core.utils.IDGenerator"))
            .transform { builder, _, _, _, _ ->
                builder.visit(Advice.to(IDGeneratorAdvice::class.java)
                    .on(ElementMatchers.named<MethodDescription>("generate")))
            }
            .installOnByteBuddyAgent()

        println("✅ Instrumentation installed")
    }
}
