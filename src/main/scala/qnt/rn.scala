package qnt


import java.time.LocalDate

import qnt.{data => qndata}


object Obj extends App {
    val fruits = List("apple", "banana", "lime", "orange")

    val fruitLengths = for {
      f <- fruits
      if f.length > 4
    } yield f.length

    Console.println(fruitLengths)

  val a = List(1,2,3,4)


  var s = a.reduce((a1,a2) => a1*a2)
  println(s)


  val lst = qndata.loadStockList(
    LocalDate.of(2007, 1, 1),
    LocalDate.of(2019, 7, 7)
  )

  val ids = lst.take(20).map(i => i.id)

  val azaza = qndata.loadStockDailySeries(ids)

  lst.filter(e => e.sector.isEmpty).foreach(println)
}