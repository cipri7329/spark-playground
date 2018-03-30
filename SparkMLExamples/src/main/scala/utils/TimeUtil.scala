package utils

object TimeUtil {
  implicit object TimeProfilerUtility {
    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block

      val nanoS = System.nanoTime - t0
      val microS = nanoS / 1000.0
      val miliS = microS / 1000.0
      val sec = miliS / 1000.0

      println(s"Elapsed time: $sec s || $miliS ms || $microS micro")
      result
    }
  }
}
