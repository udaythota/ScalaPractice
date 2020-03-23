package com.allaboutscala.practice

/**
  * Created by udaythota on 3/19/20.
  */
object ScalaBasics {
  def main(args: Array[String]) {
    println("Practicing from start again");

    // scala string interpolation
    val age = 10
    val name = "test"

    println(s"the name of the person is $name and age of the person is $age")

    // if else conditions
    val x = 20
    val res = if (x != 20) "x!=20" else "x=20"
    println(s"$res")


    // for loop
    for (x <- 1 to 3) {
      println(s"the current number using to is: $x")
      println("the current number using to is: " + x)
    }

    for (x <- 1.until(4)) {
      println("the current number using until is: " + x)
    }

    val list = List(1, 2, 3, 4, 5)
    for (x <- list) println("the list element is: " + x)

    // case match statements
    val number = 20
    val result = number match {
      case 20 => "printing from case match: " + number
      case 30 => "printing from case match: " + number
      case _ => "default"
    }
    println(result)


    // functions
    def add(x: Int, y: Int): Int = {
      return x + y
    }

    // omitting return statement
    def sub(x: Int, y: Int): Int = {
      x - y
    }

    // if you are certain about the return type of the result, you don't need to explicitly provide that
    def square(x: Int) = x * x

    def addWithDefaults(x: Int = 10, y: Int = 10) = {
      x + y
    }

    // passing without any arguments: it should take the default values
    addWithDefaults()


    // anonymous functions
    var addAnonymous = (x: Int, y: Int) => x + y
    val result2 = addAnonymous(10, 40)
    println("printing add from anonymous function: " + result2)

    // closures: functions which uses one or more variables declared outside the functions
    val outsideVariable = 20
    val closureAdd = (x: Int, y: Int) => x + y + outsideVariable

    // arrays
    val array1: Array[Int] = new Array[Int](4)
    val array2 = new Array[Int](5)

    for (x <- array1) {
      println(x)
    }

    for (i <- 0 to (array2.length - 1)) {
      println(array2(i))
    }

    val array3 = Array(1, 2, 3, 4, 5)
    for (x <- array3) {
      println(x)
    }
    val array4 = Array.concat(array1, array3)
    for (x <- array4) {
      println(x)
    }

    // lists
    val list1: List[Int] = List(1, 2, 3, 4)
    val list2: List[String] = List("test", "test1", "test2")
    println(list1)

    var sum = 0
    list.foreach(sum += _)
    println(sum)
    println(list(2))

    // maps
    val map: Map[Int, String] = {
      Map(1 -> "one", 2 -> "two", 3 -> "three", 4 -> "four")
    }
    println(map.keys)
    println(map(3))

    map.keys.foreach { key =>
      println("key is: " + key)
      println("value is: " + map(key))

    }

    // tuples
    val tuple = (1, "two", 3, "four")
    println(tuple)
    println(tuple._2)

    // tuple3 says the tuple is going to have 3 elements in it
    val tuple2 = new Tuple3(1, 2, 3)
    println(tuple2)

    // maps and flatmaps
    val list3 = List(1, 2, 3, 4)
    println(list3.map(x => x * 2))

    val list4 = List(1, 2, 3, 4, 5)
    println(list4.flatMap(x => List(x, x+1)))


    // reduce left
    list4.reduceLeft((x, y) => {
      println(x + " " + y)
      x + y
    })
  }
}
