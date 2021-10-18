package me

import org.jetbrains.kotlinx.spark.api.debugCodegen
import org.jetbrains.kotlinx.spark.api.map
import org.jetbrains.kotlinx.spark.api.sort
import org.jetbrains.kotlinx.spark.api.withSpark


fun main() {
    withSpark(
        master = "local[4]",
        props = mapOf("spark.sql.codegen.comments" to true)
    ) {
        dsOf("a" to 1, "b" to 2, "c" to 3, "d" to 4, "e" to 5, "f" to 6,
            "g" to 7, "h" to 8, "i" to 9, "j" to 10, "k" to 11, "l" to 12, "m" to 13)
            .map {
//                val la = object: Any(){
//                    fun print(): String {
//                        return ClassLoaderUtils.showClassLoaderHierarchy(this, "internal")
//                    }
//                }
//                println(la.print())
                it.second - 1 to it.first
            }
            .sort { arrayOf(it.col("first").desc()) }
            .debugCodegen()
            .show()
    }
}

/**
 * Utility class for diagnostic purposes, to analyze the
 * ClassLoader hierarchy for any given object or class loader.
 *
 * @author Rod Johnson
 * @author Juergen Hoeller
 * @since 02 April 2001
 * @see java.lang.ClassLoader
 */
object ClassLoaderUtils {
    /**
     * Show the class loader hierarchy for this class.
     * @param obj object to analyze loader hierarchy for
     * @param role a description of the role of this class in the application
     * (e.g., "servlet" or "EJB reference")
     * @param lineBreak line break
     * @param tabText text to use to set tabs
     * @return a String showing the class loader hierarchy for this class
     */
    @JvmOverloads
    fun showClassLoaderHierarchy(obj: Any, role: String, lineBreak: String = "\n", tabText: String = "\t"): String {
        val s = "object of ${obj.javaClass}: role is $role$lineBreak"
        return "$s${showClassLoaderHierarchy(obj.javaClass.classLoader, lineBreak, tabText, 0)}"
    }

    /**
     * Show the class loader hierarchy for the given class loader.
     * @param cl class loader to analyze hierarchy for
     * @param lineBreak line break
     * @param tabText text to use to set tabs
     * @param indent nesting level (from 0) of this loader; used in pretty printing
     * @return a String showing the class loader hierarchy for this class
     */
    private fun showClassLoaderHierarchy(cl: ClassLoader?, lineBreak: String, tabText: String, indent: Int): String {
        if (cl == null) {
            val ccl = Thread.currentThread().contextClassLoader
            return "context class loader=[$ccl] hashCode=${ccl.hashCode()}"
        }
        val buf = StringBuilder()
        for (i in 0 until indent) {
            buf.append(tabText)
        }
        buf.append("[").append(cl).append("] hashCode=").append(cl.hashCode()).append(lineBreak)
        val parent = cl.parent
        return buf.toString() + showClassLoaderHierarchy(parent, lineBreak, tabText, indent + 1)
    }
}
