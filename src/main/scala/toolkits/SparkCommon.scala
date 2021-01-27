package toolkits

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import timeOp.TimeCommon._

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

/**
  *
  * @author jingxuan
  * @since 2019-10-30
  *
  */

object SparkCommon {

  /**
    * generate GenericRowWithSchema out of case class
    */
  def buildRowWithSchema[T <: Product: TypeTag](record: T): GenericRowWithSchema = {
    val recordValues: Array[Any] =
      record.getClass.getDeclaredFields.map(field => {
        field.setAccessible(true)
        val returnValue = field.get(record)
        returnValue match {
          case x: Option[String] => x.get
          case _                 => returnValue
        }
      })
    new GenericRowWithSchema(recordValues, Encoders.product[T].schema)
  }

  /**
    * creating a dataset from non-header csv with given case class as Schema
    * @param spark sparkSession
    * @param inputPath file path for input csv file
    * @tparam T case class
    * @return A Dataset with pre-defined schema
    */
  def readCSV[T <: Product: TypeTag](
      spark: SparkSession,
      inputPath: String,
      delimiter: String
  ): Dataset[T] = {
    val schema = caseClassToSchema[T]
    spark.read
      .schema(schema)
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(inputPath)
      .as[T](Encoders.product[T])
  }

  /**
    * output dataframe into csv
    * @param df
    * @param outputPath
    * @param delimiter
    */
  def writeCSV(df: DataFrame, outputPath: String, delimiter: String = ",", header: Boolean = true, overwrite: Boolean = true): Unit = {
    val mode = overwrite match{
      case true => SaveMode.Overwrite
      case false => SaveMode.ErrorIfExists
    }
    df.repartition(1).write.mode(mode).option("delimter", delimiter).option("header", header).csv(outputPath)
  }

  /**
    * form Schema directly from case class
    * @tparam A a case class without values
    * @return Schema
    */
  def caseClassToSchema[A <: Product: TypeTag]: StructType =
    ScalaReflection
      .schemaFor[A]
      .dataType
      .asInstanceOf[StructType]

  /**
    * creating a dataset from parquet with given case class as Schema
    * @param spark sparkSession
    * @param inputPath file path for input parquet file
    * @tparam T case class
    * @return A Dataset with pre-defined schema
    */
  def readParquet[T <: Product: TypeTag](spark: SparkSession, inputPath: String): Dataset[T] = {
    spark.read.parquet(inputPath).as[T](Encoders.product[T])
  }

  def readAvro[T <: Product: TypeTag](spark: SparkSession, inputPath: String): Dataset[T] = {
    spark.read.format("com.databricks.spark.avro").load(inputPath).as[T](Encoders.product[T])
  }

  /**
    * explode weight table based on weight
    * @param weight
    * @param agent
    * @return
    */
  def arrayRepeat(weight: Int, agent: String): Array[String] = {
    val repeated = for (i <- 0 until weight) yield agent
    repeated.toArray
  }

  /**
    * check if dataframe has certain column
    * @param df
    * @param path
    * @return boolean
    */
  def hasColumn(df: DataFrame, path: String): Boolean = Try(df(path)).isSuccess

}

