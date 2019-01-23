package com.allaboutscala.practice

/**
  * Created by udaythota on 1/22/19.
  */
object Functions {

  def functionWithParams(price: Float, name: String): Double = {
    println(s"calculating the price using: $price and $name")
    price * 20 // last line is return by default
  }

  def functionWithParamsDefaultValue(price: Float, name: String = "defaultValue"): Double = {
    println(s"calculating the price using: $price and $name")
    price // last line is return by default
  }

  def functionWithOptionalDefaultParams(price: Float, name: String, couponCode: Option[String] = None): Double = {
    println("testing the optional code:")

    couponCode match {
      case Some("festivalPromo") =>
        println("promo applied")
        price * .20

      case _ =>
        println("no promo applied")
        price
    }
  }

  println(s"Step 1: Define a function which returns an Option of type String")

  def dailyCouponCode(): Option[String] = {
    // look up in database if we will provide our customers with a coupon today
    val couponFromDb = "COUPON_1234"
    Option(couponFromDb).filter(_.nonEmpty)
  }

  println("\nStep 4: How to define a generic typed function which will specify the type of its parameter")
  def applyDiscount[T](discount: T) {
    discount match {
      case d: String =>
        println(s"Lookup percentage discount in database for $d")

      case d: Double =>
        println(s"$d discount will be applied")

      case _ =>
        println("Unsupported discount type")
    }
  }



  def main(args: Array[String]): Unit = {
    println(functionWithParams(20, "testing"))
    println(functionWithParamsDefaultValue(20))

    println(functionWithOptionalDefaultParams(20, "test", Some("festivalPromo")))

    val couponCode = dailyCouponCode()
    println(s"the coupon code is ${couponCode.getOrElse("No coupons today")}")

    dailyCouponCode() match {
      case Some(couponCodeResult) => println(s"the coupons code is: $couponCode")
      case _ => println("no coupons available")
    }

    dailyCouponCode().map(couponCode => println(s"the coupon code is: $couponCode"))

  }

}
