package qnt

import java.time.LocalDate

import qnt.bz.{Align, DataFrame, Series}

//import org.saddle.{Series, Frame}

object stats {
  val EPS:Double = 0.0000001

  def calcTr(
                  high: DataFrame[LocalDate, String, Double],
                  low: DataFrame[LocalDate, String, Double],
                  close: DataFrame[LocalDate, String, Double]
                ): DataFrame[LocalDate, String, Double] = {
    val res = close.fillLike(Double.NaN)
    res.data.foreachKey(e => {
      val l = low.data(e)
      val h = high.data(e)
      val c = if(e._1 > 0) close.data(e._1 - 1, e._2) else Double.NaN
      val d1 = h - l
      val d2 = math.abs(h - c)
      val d3 = math.abs(c - l)
      val r = math.max(math.max(d1,d2),d3)
      res.data(e) = r
    })
    res
  }

  def calcSma(d: DataFrame[LocalDate, String, Double], period: Int): DataFrame[LocalDate, String, Double] = {
    // sma without nan correction
    val res = d.fillLike(Double.NaN)
    for(c <- d.colIdx.indices; r <- period-1 to d.rowIdx.indices.last) { //TODO transpose to speed up
      var sum = 0d
      for(i <- 0 until period) {
        val v = d.data(r-i, c)
        sum = sum + v
      }
      res.data.update(r, c, sum / period)
    }
    res
  }

  def calcSlippage
  (
    high: DataFrame[LocalDate, String, Double],
    low: DataFrame[LocalDate, String, Double],
    close: DataFrame[LocalDate, String, Double],
    period: Int = 14,
    fract: Double = 0.05d
  ): DataFrame[LocalDate, String, Double] = {
    var res = calcTr(high, low, close)
    res = calcSma(res, period)
    res.data :*= fract
    res.ffillRows(Double.NaN)
  }

  def calcRelativeReturn(
                           inOpen: DataFrame[LocalDate, String, Double],
                           inHigh: DataFrame[LocalDate, String, Double],
                           inLow: DataFrame[LocalDate, String, Double],
                           inClose: DataFrame[LocalDate, String, Double],
                           inDivs: DataFrame[LocalDate, String, Double],
                           inLiquid: DataFrame[LocalDate, String, Double],
                           inWeights: DataFrame[LocalDate, String, Double],
                           slippageFactor: Double = 0.05d
                         ): Series[LocalDate, Double] = {

    val slippageWhole = calcSlippage(inHigh, inLow, inClose, fract=slippageFactor)

    val firstRowSlippage = slippageWhole.firstNotEmptyRow(Double.NaN)
    val firstRowWeight = inWeights.firstNotEmptyRow(Double.NaN)
    val firstRow = if(firstRowSlippage.compareTo(firstRowWeight) > 0) firstRowSlippage else firstRowWeight

    val slippage = slippageWhole.rowOps.locRange(firstRow, slippageWhole.rowIdx.last, 1).copy

    val weights = data.normalizeOutput(inWeights).shiftRows(1, 0).align(slippage, Align.right, Double.NaN)
    val open = inOpen.align(slippage, Align.right, Double.NaN)
    val openff = open.ffillRows(Double.NaN).fillMissed(Double.NaN, 0d)
    val divs = inDivs.align(slippage, Align.right, Double.NaN).fillMissed(Double.NaN, 0)
    val liquid = inLiquid.align(slippage, Align.right, Double.NaN).fillMissed(Double.NaN, 0d)
    val close = inClose.align(slippage, Align.right, Double.NaN)
    val closeff = close.ffillRows(Double.NaN).fillMissed(Double.NaN, 0d)

    val N = Series.fill(weights.colIdx, 0d) // shares count
    val prevN = Series.fill(weights.colIdx, 0d) // prev shares count
    val unlocked = Series.fill(weights.colIdx, false)

    var equityBeforeBuy = 1d
    var equityAfterBuy = 1d
    var equityTonight = 1d

    val relativeReturns = Series.fill(weights.rowIdx, 0d)

    for(ti <- 0 to weights.rowIdx.indices.last) {

      for(ai <- weights.colIdx.indices) {
        var u = true
        u &&= slippage.data(ti, ai).isFinite
        u &&= weights.data(ti, ai).isFinite
        u &&= open.data(ti, ai).isFinite && open.data(ti, ai) > EPS
        u &&= close.data(ti, ai).isFinite
        unlocked.data(ai) = u
      }

      equityBeforeBuy = equityAfterBuy
      for (ai <- weights.colIdx.indices) {
        equityBeforeBuy += (openff.data(ti, ai) - openff.data(math.max(0, ti - 1), ai) + divs.data(ti, ai)) * N.data(ai)
      }

      var wSum = 0d
      var wUnlocked = 0d
      var unoperableEquity = 0d
      for(ai <- weights.colIdx.indices) {
        val w = weights.data(ti, ai)
        if (!w.isNaN) {
          wSum += math.abs(w)
        }
        if(unlocked.data(ai)) {
          wUnlocked += math.abs(w)
        } else {
          unoperableEquity += math.abs(openff.data(ti, ai) * N.data(ai))
        }
      }
      val wFreeCash = math.max(1, wSum) - wSum
      val wOperable = wUnlocked + wFreeCash
      var equityOperableBeforeBuy = equityBeforeBuy - unoperableEquity

      prevN.data := N.data

      if(wOperable < EPS) {
        equityAfterBuy = equityBeforeBuy
      } else {
        var S = 0d // current slippage

        for (ai <- weights.colIdx.indices) {
          if (unlocked.data(ai)) {
            if (liquid.data(ti, ai) > 0)
              N.data(ai) = equityOperableBeforeBuy * weights.data(ti, ai) / (wOperable * open.data(ti, ai))
            else
              N.data(ai) = 0
            S += slippage.data(ti, ai) * math.abs(N.data(ai) - prevN.data(ai))
          }
        }
        equityAfterBuy = equityBeforeBuy - S
      }

      var growthTonight = 0d
      for (ai <- weights.colIdx.indices) {
        growthTonight += N.data(ai) * (closeff.data(ti, ai) - openff.data(ti, ai))
      }

      var prevEquityTonight = equityTonight
      equityTonight = equityAfterBuy + growthTonight
      relativeReturns.data(ti) =  equityTonight/prevEquityTonight - 1

      println(
        "t", ti,
        "wSum", wSum,
        "w_free_cash", wFreeCash,
        "w_unlocked", wUnlocked,
        "w_operable", wOperable,
        "equity_before_buy", equityBeforeBuy,
        "equity_after_buy", equityAfterBuy
      )
    }

    relativeReturns
  }

  def calcStdDev[I](
                     series: Series[I, Double],
                     max_periods: Int = 252,
                     min_periods: Int = 2
                   ): Series[I, Double] = {
    val stddev = series.fillLike(Double.NaN)
    for(i <- min_periods - 1 to series.idx.indices.last) {
      val start = math.max(0, (i - max_periods + 1))
      var mean = 0d
      for(j <- start to i) {
        mean += series.data(j)
      }
      mean /= (i - start + 1)
      var sum = 0d
      for(j <- start to i) {
        val v = series.data(j)
        sum += (v - mean)*(v - mean)
      }
      stddev.data(i) = math.sqrt(sum / (i - start))
    }
    stddev
  }

  def calcVolatilityAnnualized(
                                relativeReturns: Series[LocalDate, Double],
                                max_periods: Int = 252,
                                min_periods: Int = 2
                              ): Series[LocalDate, Double] = {
    var volatility = calcStdDev(relativeReturns, max_periods, min_periods)
    volatility.data :*= math.sqrt(252) // annualization
    volatility
  }

  def calcMeanReturnAnnualized(
                                relativeReturns: Series[LocalDate, Double],
                                max_periods: Int = 252,
                                min_periods: Int = 2
                              ): Series[LocalDate, Double] = {
    val mr = relativeReturns.fillLike(Double.NaN)
    for(i <- min_periods - 1 to relativeReturns.idx.indices.last) {
      val start = math.max(0, (i - max_periods + 1))
      var mean = 0d
      for(j <- start to i) {
        mean += math.log(relativeReturns.data(j) + 1)
      }
      mean /= (i - start + 1)
      mean = math.exp(mean) - 1d

      mean = math.pow(mean + 1, 252) - 1 // annualization

      mr.data(i) = mean
    }
    mr
  }

  def calcSharpeRatio(
                       relativeReturns: Series[LocalDate, Double],
                       max_periods: Int = 252,
                       min_periods: Int = 2
                   ): Series[LocalDate, Double] = {
    val volatility = calcVolatilityAnnualized(relativeReturns, max_periods, min_periods)
    val meanReturn = calcMeanReturnAnnualized(relativeReturns, max_periods, min_periods)
    val sharpeRatio = relativeReturns.fillLike(Double.NaN)
    for(i <- relativeReturns.idx.indices) {
      sharpeRatio.data(i) = meanReturn.data(i) / volatility.data(i)
    }
    sharpeRatio
  }
}
