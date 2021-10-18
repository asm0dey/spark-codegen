fun main() {
    withSpark {
        dsOf("a" to 1, "b" to 2, "c" to 3, "d" to 4, "e" to 5, "f" to 6,
            "g" to 7, "h" to 8, "i" to 9, "j" to 10, "k" to 11, "l" to 12, "m" to 13)
            .map { it.second - 1 to it.first }
            .sort { arrayOf(it.col("first").desc()) }
            .show()
    }
}
