data class Q<T>(val id: Int, val text: T)
object Main {
    @JvmStatic
    fun main(args: Array<String>) {

        val spark = SparkSession
            .builder()
            .master("local[2]")
            .appName("Simple Application").orCreate

        val triples = spark
            .toDS(listOf(Q(1, 1 to null), Q(2, 2 to "22"), Q(3, 3 to "333")))
            .map { (a, b) -> a + b.first to b.second?.length }
            .map { it to 1 }
            .map { (a, b) -> Triple(a.first, a.second, b) }


        val pairs = spark
            .toDS(listOf(2 to "hell", 4 to "moon", 6 to "berry"))

        triples
            .leftJoin(pairs, triples.col("first").multiply(2).eq(pairs.col("first")))
            .map { (triple, pair) -> Five(triple.first, triple.second, triple.third, pair?.first, pair?.second) }
            .groupByKey { it.a }
            .reduceGroupsK { v1, v2 -> v1.copy(a = v1.a + v2.a, b = v1.a + v2.a) }
            .map { it.second }
            .repartition(1)
            .withCached {
                write()
                    .also { it.csv("csvpath") }
                    .also { it.orc("orcpath") }
                showDS()
            }



        spark.stop()
    }

    data class Five<A, B, C, D, E>(val a: A, val b: B, val c: C, val d: D, val e: E)
}
