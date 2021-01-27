import org.scalatest.FlatSpec
import timeOp.TimeCommon.{dateRange,toDate,toOtherTZ}
import timeOp.TimeCommon._
/**
  *
  * @author jingxuan
  * @since 31/3/20
  *
  */

class TimeCommonSpec extends FlatSpec{
  "TimeCommon" should "generate list of dates" in {
    val dates = dateRange("2020-01-01", "2020-01-02", 1).toList
    assert(dates == List("2020-01-01", "2020-01-02"))
  }
  "TimeCommon" should "output toOtherTZ as String" in {
    val zdtStr = "2020-01-02 10:00:00+0000"
    val res: String = toOtherTZ(zdt = zdtStr, timeZone = "Australia/Sydney")
    assert(res == "2020-01-02 21:00:00+1100")
  }
  "TimeCommon" should "generate date range" in {
    val dates = dateRange("2020-01-01", "2020-01-30", 1)
    dates.foreach(println)
  }
}
