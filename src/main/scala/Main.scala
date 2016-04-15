import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

case class Access(timestamp: DateTime, ipAddress: String)
case class Session(ipAddress: String, accessCount: Int, duration: Long)

object Main {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val in = sys.env("INPUT_FILE")
    val out = sys.env("OUTPUT_FILE")

    val accessLogs: RDD[String] = sc.textFile(in)
    accessLogs
      .map { line =>
        val fields = line.split(' ')
        Access(DateTime.parse(fields(0)), fields(1))
      }
      .groupBy(_.ipAddress)
      .flatMap { case (ipAddress, accesses) =>
        val sorted = accesses.toList.sortBy(_.timestamp)
        sessionize(ipAddress, sorted)
      }
      .map(s => s"${s.ipAddress} ${s.accessCount} ${s.duration}")
      .saveAsTextFile(out)
  }

  def sessionize(ipAddress: String, sortedAccesses: Seq[Access]): Seq[Session] = {
    @tailrec
    def loop(
      ipAddress: String,
      accesses: Seq[Access],
      lastAccess: Option[Access],
      accessesInSession: Seq[Access],
      result: Seq[Session]): Seq[Session] = accesses match {
      case head +: tail =>
        val lastTimestamp = lastAccess.getOrElse(head).timestamp
        if (lastTimestamp + 30.minutes < head.timestamp) {
          val duration = accessesInSession.last.timestamp to accessesInSession.head.timestamp
          val session = Session(head.ipAddress, accessesInSession.size, duration.toDuration.seconds)
          loop(ipAddress, tail, Some(head), Nil, session +: result)
        } else {
          loop(ipAddress, tail, Some(head), head +: accessesInSession, result)
        }
      case _ =>
        val duration = accessesInSession.last.timestamp to accessesInSession.head.timestamp
        val session = Session(ipAddress, accessesInSession.size, duration.toDuration.seconds)
        session +: result
    }

    loop(ipAddress, sortedAccesses, None, Nil, Nil)
  }
}
