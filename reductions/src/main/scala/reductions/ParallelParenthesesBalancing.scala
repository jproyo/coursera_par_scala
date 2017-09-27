package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {

    @tailrec
    def verify(idx: Int, until: Int, count: Int): Int = {
      if (idx == until) count
      else if (count < 0) count
      else {
        chars(idx) match {
          case '(' => verify(idx + 1, until, count + 1)
          case ')' => verify(idx + 1, until, count - 1)
          case _ =>  verify(idx + 1, until, count)
        }
      }
    }
    if (chars.length == 0) true
    else verify(0, chars.length, 0) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    @tailrec
    def traverse(idx: Int, until: Int, cOpen: Int, cClosed: Int): (Int,Int) = {
      if(idx == until) (cOpen, cClosed)
      else {
        chars(idx) match {
          case '(' => traverse(idx+1, until, cOpen+1, cClosed)
          case ')' => {
            if (cOpen > 0) traverse(idx+1, until, cOpen-1, cClosed)
            else traverse(idx+1, until, cOpen, cClosed+1)
          }
          case _ => traverse(idx+1, until, cOpen, cClosed)
        }
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until, 0, 0)
      } else {
        val mid = from + (until - from) / 2
        val ((a1, a2), (b1, b2)) = parallel(reduce(from, mid),
          reduce(mid, until))
        (a1-b2, a2-b1)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
