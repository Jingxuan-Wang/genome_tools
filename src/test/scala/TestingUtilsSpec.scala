import org.scalatest.FlatSpec
import toolkits.TestingUtils.{test1,test2}

/**
  *
  * @author jingxuan
  * @since 2019-09-18
  *
  */

class TestingUtilsSpec extends FlatSpec {
  "TestingUtils.Test" should "be able to take function as input and test it" in {
    val fx = 1 / (_:Int)
    val result = test1[Int](fx)(0)

    assert(!result)
  }
  "TesintUtils.Test" should "be able to take two parameters" in {
    val fx = (_:String) + (_: String)
    val result = test2[String, String](fx)("a", "b")
    assert(result)
  }
}
