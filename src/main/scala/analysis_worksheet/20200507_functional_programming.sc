/**
* Example for functional programming
*
* @author: 
* @date: 7/5/20
* @project: 
*
*/

// high order functions
def sum(f: Int => Int, a: Int, b: Int): Int = {
  f(a) + f(b)
}

//anonymous function
(i: Int) =>{i + 1}

def sumInt(a: Int, b: Int): Int = sum(x => x, a, b)

def sumCube(a: Int, b: Int): Int = sum(x => x * x * x, a, b)

