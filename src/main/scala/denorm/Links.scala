package denorm

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  *
  * @author jingxuan
  * @since 2019-11-15
  *
  */

object Links {

  val appName = this.getClass.getSimpleName

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Params](appName) {
      opt[String]("input")
        .text("Input directory")
        .action((x, c) => c.copy(inputPath = x))

      opt[String]("output")
        .text("Output directory")
        .action((x, c) => c.copy(outputPath = x))

      opt[String]("state")
        .text("State")
        .action((x, c) => c.copy(state = x))
    }
    parser.parse(args, Params()).map { params =>
      process(params)
    }
  }

  def process(params: Params): Unit = {
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    import spark.implicits._
    val input = spark.read.format("com.databricks.spark.avro").load(params.inputPath)

    val selected = input
      .where($"mode" === "TRAIN" || $"mode" === "RAIL")
      .where($"link_id".contains(params.state))

    val res = selected.select($"agent_id", $"mode", $"link_id")

    res.write.parquet(params.outputPath)

    spark.stop()
  }

  case class Params(
    inputPath: String = "",
    outputPath: String = "",
    state: String = ""
  )

}
