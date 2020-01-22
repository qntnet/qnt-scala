//package qnt
//
//
//import java.time.LocalDate
//
//import qnt.{data => qndata}
//
//
//object Obj extends App {
//    val fruits = List("apple", "banana", "lime", "orange")
//
//    val fruitLengths = for {
//      f <- fruits
//      if f.length > 4
//    } yield f.length
//
//    Console.println(fruitLengths)
//
//  val a = List(1,2,3,4)
//
//
//  var s = a.reduce((a1,a2) => a1*a2)
//  println(s)
//
//
//  val lst = qndata.loadStockList(
//    LocalDate.of(2007, 1, 1),
//    LocalDate.of(2019, 7, 7)
//  )
//
//  var si : qndata.StockInfo = lst(0);
//
//  val ids = lst.map(i => i.id)
//
//  val azaza = qndata.loadStockDailySeries(ids)
//
//  print("done")
//
//
//  // memory info
//  val mb = 1024*1024
//  val runtime = Runtime.getRuntime
//  //runtime.gc()
//  println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
//  println("** Free Memory:  " + runtime.freeMemory / mb)
//  println("** Total Memory: " + runtime.totalMemory / mb)
//  println("** Max Memory:   " + runtime.maxMemory / mb)
//
//  //  lst.filter(e => e.sector.isEmpty).foreach(println)
//
//
//
//}