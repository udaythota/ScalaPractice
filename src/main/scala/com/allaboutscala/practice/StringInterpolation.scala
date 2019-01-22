package com.allaboutscala.practice

/**
  * Created by udaythota on 1/21/19.
  */
object StringInterpolation {

  val immutable: String = "this is immutable: cannot be reassigned"

  // immutable = "compilation error"

  var mutable: String = "this is mutable: can be reassigned"
  mutable = "string mutated"

  case class Donut(name: String, taste: String, price: Integer)

  val newDonut = Donut("vanilla donut", "sweet", 20)

  // auto type inference from scala
  val numberOfDonuts = 20

  val donutPrice = 2

  // when reassigning to a new variable, explicit type conversion is needed
  val numberOfDonutsInString: String = numberOfDonuts.toString


  def main(args: Array[String]): Unit = {
    println("mutable string should be the new one after reassignment: " + mutable)
    println(s"new donut name: ${newDonut.name}, taste level is: ${newDonut.taste}, price is ${newDonut.price}")
    println(s"is the price of the donut 20: ${newDonut.price == 20}")

    println("Testing if else:")
    if (numberOfDonuts < 20) {
      println(s"total price is: ${
        numberOfDonuts * donutPrice
      }")
    }
    else if (numberOfDonuts == 20) {
      println(s"total price is:  ${
        numberOfDonuts * (donutPrice - 1)
      }")
    }
    else {
      println(s"total price is:  ${
        numberOfDonuts * 1
      }")
    }

    println("testing if else as a statement: ")
    val newDonutPrice = if (numberOfDonuts == 20) numberOfDonuts * donutPrice else numberOfDonuts * 1 // equivalent of ternary operator in java
    println(s"new donut price is: $newDonutPrice")


    println("testing simple for loop:")
    for (numberOfDonuts <- 1 until 5) {
      println(s"the number of donuts is: $numberOfDonuts")
    }


    println("testing simple for loop, last element inclusive:")
    for (test <- 1 to 5) {
      println(s"the number of donuts is: $test")
    }

    println("testing for loop with if condition: ")
    val testList = List("sugar", "honey", "sweet", "cane-sugar")
    for (sugarType <- testList if sugarType == "sugar") {
      println(s"found ingredient = $sugarType")
    }

    println("testing for loop with yield:")
    val testList1 = List("sugar", "honey", "sweet", "cane-sugar")
    val resultList = for {
      sugarType <- testList1 if sugarType == "sugar" || sugarType == "honey"
    } yield sugarType
    println(s"result list is: $resultList")


    println("testing ranges and lists:")
    val listFrom1To5 = (1 to 5).toList
    println(s"Range to sequence = ${listFrom1To5.mkString(" ")}")

    listFrom1To5.foreach(print(_))

  }

}
