// scala default libs

/**
* learning scala object
*
* @author: jingxuan
* @date: 11/5/20
* @project: learning
*
*/

// protected will limit their access to code from the same class or its subclasses.
class User {protected val passwd = util.Random.nextString(10)}

class ValidUser extends User {def isValid = !passwd.isEmpty}

val isValid = new ValidUser().isValid

// testing protected val
val user = new User()
println(user.passwd)

// private will limit the access to code from the same class
class User2(private var password: String){
  def update(p: String): Unit ={
    println("Modifying the password!")
    password = p
  }

  def validate(p:String): Boolean = p == password
}

// implicit
object ImplicitClasses{

}

