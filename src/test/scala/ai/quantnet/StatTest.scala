package ai.quantnet

import java.time.LocalDateTime

import org.scalatest.FunSuite

class StatTest extends FunSuite {

  test("bnh") {

    val dataSet = data.loadCryptocurrencyHourlySeries(
      minDate = LocalDateTime.of(2015, 1, 1,0,0,0),
     // maxDate = LocalDateTime.of(2018, 1, 1,0,0,0),
    )
    var output = dataSet(data.fields.close).copy
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

    val srr = s.colOps.get(stats.fields.relativeReturn)

    println(s.toString(headRows = 20))

    //println(stats.calcCorrelation(rr))

//
//    var ob = netcdf.dataFrameToNetcdf(output)
//
//    var o2 = netcdf.netcdf2DToDailyFrames(ob)
//
//    print(o2)

    //data.writeOutput(output)
  }

}
