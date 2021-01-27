package toolkits

/**
  *
  * @author jingxuan
  * @since 2019-09-18
  *
  */

object TestingUtils {

  /**
    * This function takes input function with one parameter to test which line break the input
    * function
    * @param fx function need to be tested
    * @param input input parameter for the function fx
    * @tparam A input type
    * @return False if the function failed
    */
  def test1[A](fx: (A) => Any)(input: A): Boolean = {
    try {
      fx(input)
      true
    } catch {
      case e: Exception => false
    }
  }

  def test2[A, B](fx: (A, B) => Any)(input1: A, input2: B): Boolean = {
    try {
      fx(input1, input2)
      true
    } catch {
      case e: Exception => false
    }
  }
}
