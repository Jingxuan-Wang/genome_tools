package toolkits

import scala.io.Source
import java.io._
import scala.math.ceil
/**
  *
  * @author jingxuan
  * @since 2019-05-30
  *
  */

object Utils {

  def readTextFile(fPath: String): Seq[String] ={
    Source.fromFile(fPath).getLines().toList
  }

  def writeTextFile(s: Seq[String], filename:String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    s.map(i => bw.write(i+"\n"))
    bw.close()
  }

  def percentile(lis: Seq[Double], percent: Double): Double = {
    val sortedList = lis.sortBy(x => x)
    val indx: Int = ceil(percent*sortedList.length).toInt
    sortedList(indx - 1)
  }


}
