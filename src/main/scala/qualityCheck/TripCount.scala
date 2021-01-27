package qualityCheck

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  *
  * @author jingxuan
  * @since 2019-10-17
  *
  */

object TripCount {
  case class Params(
  inputPath: String = "",
  outputPath: String = ""
  )

  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {
      opt[String]("input")
        .text("Input directory")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("output")
        .text("Output directory")
        .action((x, c) => c.copy(outputPath = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }

    def process(params: Params): Unit = {
      val spark = SparkSession.builder().appName(appName).getOrCreate()
      import spark.implicits._
      val df = spark.read.load(params.inputPath)
      val selectedDF = df.where($"category" === "sa2_sa2")

      selectedDF.coalesce(1).write.mode("append").csv(params.outputPath)
      spark.stop()
    }
  }
}
