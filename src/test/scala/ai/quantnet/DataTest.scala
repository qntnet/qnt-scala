package ai.quantnet

import java.time.LocalDate

import org.scalatest.FunSuite

class DataTest extends FunSuite {

  test("loadCruptocurrency") {
    val d = data.loadCryptocurrencyHourlySeries()
    print(d)
    assert(d.nonEmpty)
  }

  test("loadStockList") {
    val stocks = data.loadStockList()
    assert(stocks.nonEmpty)
    assert(stocks.forall(s => s.id.contains(":")))
  }

  test("loadStockDailySeries") {
    val startDate = LocalDate.of(2007,1,1)
    val finishDate =  LocalDate.of(2019,1,1)
    val stocks = data.loadStockList(startDate, finishDate)
    val ids = stocks.take(2000).map(i => i.id)
    val series = data.loadStockDailySeries(ids, startDate, finishDate)
    assert(series.keys.toSet == data.fields.values)
    assert(series.values.forall(s => s.colIdx.toIndexedSeq.forall(id => ids.contains(id))))
    assert(series.values.forall(s => s.rowIdx.toIndexedSeq.forall(t => t.compareTo(startDate) >= 0 && t.compareTo(finishDate) <= 0)))

    val mb = 1024*1024
    val runtime = Runtime.getRuntime
    //runtime.gc()
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)
  }

  test("loadIndexList") {
    val lst = data.loadIndexList()
    assert(lst.nonEmpty)
  }

  test("loadIndexDailySeries") {
    val startDate = LocalDate.of(2017,1,1)
    val finishDate =  LocalDate.of(2019,1,1)
    val idxList = data.loadIndexList(startDate, finishDate)
    val ids = idxList.take(50).map(i => i.id)
    val series = data.loadIndexDailySeries(ids, startDate, finishDate)
    assert(series.colIdx.toIndexedSeq.forall(id => ids.contains(id)))
    assert(series.rowIdx.toIndexedSeq.forall(t => t.compareTo(startDate) >= 0 && t.compareTo(finishDate) <= 0))
  }

  test("load_secgov_forms") {
    val startDate = LocalDate.of(2007,1,1)
    val finishDate =  LocalDate.of(2019,1,1)
    val stocks = data.loadStockList(startDate, finishDate)
    val ciks = stocks.map(_.cik).filter(_.isDefined).map(_.get)
    var forms = data.loadSecgovForms(ciks)
    assert(forms.nonEmpty)

    val mb = 1024*1024
    val runtime = Runtime.getRuntime
    //runtime.gc()
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)
  }
}
