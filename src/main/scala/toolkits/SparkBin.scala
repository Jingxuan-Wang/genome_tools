package toolkits

/**
  *
  * @author jingxuan
  * @since 17/3/20
  *
  */
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame

object SparkBin {
  def equalBin(lowerBound: Double, upperBound: Double, interval: Double, inputCol: String,
               outputCol: String, dataframe: DataFrame): DataFrame = {
    val bins = (lowerBound to upperBound by interval).toArray
    val bucketizer = new Bucketizer().setInputCol(inputCol).setOutputCol(outputCol).setSplits(bins)
    val binnedDF = bucketizer.transform(dataframe)
    binnedDF
  }

  def customizeBin(bins: Array[Double], inputCol: String, outputCol: String,
                   dataframe: DataFrame): DataFrame = {
    val bucketizer = new Bucketizer().setInputCol(inputCol).setOutputCol(outputCol).setSplits(bins)
    val binnedDF = bucketizer.transform(dataframe)
    binnedDF
  }
}
