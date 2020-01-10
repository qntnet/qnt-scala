package qnt

import java.time.LocalDate

import org.scalatest.FunSuite
import qnt.bz.DataFrame

class StatTest extends FunSuite {

  test("bnh") {

    val dataSet = data.loadStockDailySeries(
      minDate = LocalDate.of(2015, 1, 1),
      maxDate = LocalDate.of(2018, 1, 1),
    )
    var output = dataSet(data.fields.is_liquid).copy
    output = data.normalizeOutput(output)
    output.data :*= -1d

    println(output)
    val rr = stats.calcRelativeReturn(
      dataSet(data.fields.open),
      dataSet(data.fields.high),
      dataSet(data.fields.low),
      dataSet(data.fields.close),
      dataSet(data.fields.divs),
      dataSet(data.fields.is_liquid),
      output,
      slippageFactor = 0.05d
    )

    val slippage = stats.calcSlippage(dataSet(data.fields.high),dataSet(data.fields.low),dataSet(data.fields.close))
    //println(slippage.rowOps.ilocRange(13, slippage.rowIdx.length))

    println("stats")
    val v = stats.calcVolatilityAnnualized(rr, 252*3)
    val mr = stats.calcMeanReturnAnnualized(rr, 252*3)
    val sr = stats.calcSharpeRatio(rr, 252*3)
    val df = DataFrame.fromSeries(
      ("rr", rr),
      ("mr", mr),
      ("v", v),
      ("sr", sr)
    )

    println(df.toString(headRows = 20))
  }

}
