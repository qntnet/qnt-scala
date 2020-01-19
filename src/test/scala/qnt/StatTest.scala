package qnt

import java.time.LocalDate

import org.scalatest.FunSuite

class StatTest extends FunSuite {

  test("bnh") {

    val dataSet = data.loadStockDailySeries(
      minDate = LocalDate.of(2015, 1, 1),
      maxDate = LocalDate.of(2018, 1, 1),
    )
    var output = dataSet(data.fields.is_liquid).copy
    output = data.normalizeOutput(output)
    output.data :*= 1d

    println(output)
    val rr = stats.calcRelativeReturn(
      dataSet,
      output,
      slippageFactor = 0.05d
    )

    val slippage = stats.calcSlippage(dataSet(data.fields.high),dataSet(data.fields.low),dataSet(data.fields.close))
    //println(slippage.rowOps.ilocRange(13, slippage.rowIdx.length))

    println("stats")

    val s = stats.calcStats(dataSet, output)

    println(s.toString(headRows = 20))

    //println(stats.calcCorrelation(rr))


    var ob = data.dataFrameToNetcdf(output)

    var o2 = data.netcdf2DToFrames(ob)

    print(o2)

    data.writeOutput(output)
  }

}
