package qnt

import java.time.LocalDate

import qnt.bz.{Align, DataFrame, Series}

import scala.util.control.Breaks._

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
    for(c <- d.colIdx.indices; r <- period-1 to d.rowIdx.indices.last) {
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
                           /*inOpen: DataFrame[LocalDate, String, Double],
                           inHigh: DataFrame[LocalDate, String, Double],
                           inLow: DataFrame[LocalDate, String, Double],
                           inClose: DataFrame[LocalDate, String, Double],
                           inDivs: DataFrame[LocalDate, String, Double],
                           inLiquid: DataFrame[LocalDate, String, Double],
                           */
                           inData: Map[String, DataFrame[LocalDate, String, Double]],
                           inWeights: DataFrame[LocalDate, String, Double],
                           slippageFactor: Double = 0.05d
                         ): Series[LocalDate, Double] = {

    val slippageWhole = calcSlippage(inData(data.fields.high), inData(data.fields.low), inData(data.fields.close), fract=slippageFactor)

    val firstRowSlippage = slippageWhole.firstNotEmptyRow(Double.NaN)
    val firstRowWeight = inWeights.firstNotEmptyRow(Double.NaN)
    val firstRow = if(firstRowSlippage.compareTo(firstRowWeight) > 0) firstRowSlippage else firstRowWeight

    val slippage = slippageWhole.rowOps.locRange(firstRow, slippageWhole.rowIdx.last, 1).copy

    val weights = data.normalizeOutput(inWeights).shiftRows(1, 0).align(slippage, Align.right, Double.NaN)
    val open = inData(data.fields.open).align(slippage, Align.right, Double.NaN)
    val openff = open.ffillRows(Double.NaN).fillMissed(Double.NaN, 0d)
    val divs = inData(data.fields.divs).align(slippage, Align.right, Double.NaN).fillMissed(Double.NaN, 0)
    val liquid = inData(data.fields.is_liquid).align(slippage, Align.right, Double.NaN).fillMissed(Double.NaN, 0d)
    val close = inData(data.fields.close).align(slippage, Align.right, Double.NaN)
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

//      println(
//        "t", ti,
//        "wSum", wSum,
//        "w_free_cash", wFreeCash,
//        "w_unlocked", wUnlocked,
//        "w_operable", wOperable,
//        "equity_before_buy", equityBeforeBuy,
//        "equity_after_buy", equityAfterBuy
//      )
    }

    relativeReturns
  }

  def calcSma[I](
                     series: Series[I, Double],
                     max_periods: Int = 252,
                     min_periods: Int = 2
                   ): Series[I, Double] = {
    val sma = series.fillLike(Double.NaN)
    for(i <- min_periods - 1 to series.idx.indices.last) {
      val start = math.max(0, (i - max_periods + 1))
      var mean = 0d
      for(j <- start to i) {
        mean += series.data(j)
      }
      mean /= (i - start + 1)
      sma.data(i) = mean
    }
    sma
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
                                maxPeriods: Int = 252,
                                minPeriods: Int = 2
                              ): Series[LocalDate, Double] = {
    var volatility = calcStdDev(relativeReturns, maxPeriods, minPeriods)
    volatility.data :*= math.sqrt(252) // annualization
    volatility
  }

  def calcMeanReturnAnnualized(
                                relativeReturns: Series[LocalDate, Double],
                                maxPeriods: Int = 252,
                                minPeriods: Int = 2
                              ): Series[LocalDate, Double] = {
    val mr = relativeReturns.fillLike(Double.NaN)
    for(i <- minPeriods - 1 to relativeReturns.idx.indices.last) {
      val start = math.max(0, (i - maxPeriods + 1))
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
                       maxPeriods: Int = 252,
                       minPeriods: Int = 2
                   ): Series[LocalDate, Double] = {
    val volatility = calcVolatilityAnnualized(relativeReturns, maxPeriods, minPeriods)
    val meanReturn = calcMeanReturnAnnualized(relativeReturns, maxPeriods, minPeriods)
    val sharpeRatio = relativeReturns.fillLike(Double.NaN)
    for(i <- relativeReturns.idx.indices) {
      sharpeRatio.data(i) = meanReturn.data(i) / volatility.data(i)
    }
    sharpeRatio
  }

  def calcEquity(relativeReturns: Series[LocalDate, Double]): Series[LocalDate, Double] = {
    val result = relativeReturns.fillLike(Double.NaN)
    var equity = 1d
    for(i <- relativeReturns.idx.indices) {
      equity *= 1d + relativeReturns.data(i)
      result.data(i) = equity
    }
    result
  }

  def calcUnderwater(equity: Series[LocalDate, Double]): Series[LocalDate, Double] = {
    var maxEquity = 1d
    val result = equity.fillLike(Double.NaN)
    for(i <- equity.idx.indices) {
      val e = equity.data(i)
      maxEquity = math.max(maxEquity, e)
      result.data(i) = e/maxEquity - 1
    }
    result
  }

  def calcMaxDrawdown(underwater: Series[LocalDate, Double]): Series[LocalDate, Double] = {
    var maxDD = 0d
    val result = underwater.fillLike(Double.NaN)
    for(i <- underwater.idx.indices) {
      maxDD = math.min(maxDD, underwater.data(i))
      result.data(i) = maxDD
    }
    result
  }

  def calcBias(inWeight: DataFrame[LocalDate, String, Double]): Series[LocalDate, Double] = {
    val weight = data.normalizeOutput(inWeight)
    val b = Series.fill(weight.rowIdx, Double.NaN)
    for(ri <- weight.rowIdx.indices) {
      var sum = 0d
      var absSum = 0d
      for(ci <- weight.colIdx.indices) {
        val v = weight.data(ri, ci)
        sum += v
        absSum += math.abs(v)
      }
      b.data(ri) = if(absSum > 0) sum/absSum else 0
    }
    b
  }

  def calcSectorDistribution(inWeights: DataFrame[LocalDate, String, Double]) : DataFrame[LocalDate, String, Double] = {
    var weights = data.normalizeOutput(inWeights)
    var sectors = Map[String, Series[LocalDate, Double]]()
    var infos = data.loadStockList(weights.rowIdx(0), weights.rowIdx.last).map(e => e.id -> e).toMap

    for(a <- weights.colIdx){
      breakable {
        if (!infos.contains(a))  {
         break
        }
        val info = infos(a)
        val sector = if(info.sector.isEmpty) "Other" else info.sector.get

        val sa = weights.rowOps.get(a)
        if(sa.data.forall(v => v.isNaN || v.equals(0d))) {
          break
        }
        if(!sectors.contains(sector)) {
          val s =  weights.colOps.iget(0).fillLike(0d)
          sectors += (sector -> s)
        }
        val ss = sectors(sector)


        for(ai <- sa.idx.indices) {
          val v = sa.data(ai)
          if (!v.isNaN && !v.equals(0d)) {
            ss.data(ai) += v
          }
        }
      }
    }

    DataFrame.fromSeriesColumns(sectors.iterator.toSeq.sortBy(_._1))
  }

  def calcInstruments(weights: DataFrame[LocalDate, String, Double]): Series[LocalDate, Double] = {
    val icnt = weights.fillMissed(Double.NaN, 0d)
    for(ci <- weights.colIdx.indices; ri <- 1 to weights.rowIdx.indices.last) {
      icnt.data.update(ri, ci, if(icnt.data(ri, ci) != 0 || icnt.data(ri-1, ci) != 0) 1d else 0d)
    }
    val result = Series.fill(weights.rowIdx, 0d)
    for(ri <- 1 to weights.rowIdx.indices.last) {
      var sum = 0d
      for(ci <- weights.colIdx.indices) {
        sum += icnt.data(ri, ci)
      }
      result.data(ri) = sum
    }
    result
  }

  def calcAvgTurnover(
                       inOpen: DataFrame[LocalDate, String, Double],
                       inWeights: DataFrame[LocalDate, String, Double],
                       inEquity: Series[LocalDate, Double],
                       maxPeriods: Int = 252, minPeriods: Int = 1,
                     ): Series[LocalDate, Double] = {
    val W = data.normalizeOutput(inWeights.align(inEquity.idx, inWeights.colIdx, 0d)).shiftRows(1, 0)
    val pW = W.shiftRows(1, 0)

    val open = inOpen.align(W, Align.right, Double.NaN)
    val pOpen = open.shiftRows(1, Double.NaN)

    val E = inEquity
    val pE = E.shift(1, 1d)

    val turnover = E.fillLike(0d)


    for(ti <- W.rowIdx.indices) {
      var turn = 0d
      for(ai <- W.colIdx.indices) {
        val dt = W.data.apply(ti, ai) - pW.data.apply(ti, ai) * pE.data(ti) * open.data(ti, ai) / (pOpen.data(ti, ai) * E.data(ti))
        if(dt.isFinite) turn += math.abs(dt)
      }
      turnover.data(ti) = turn
    }

    calcSma(turnover, maxPeriods, minPeriods)
  }

  def calcCorrelation(relativeReturns: Series[LocalDate, Double]): Series[LocalDate, Double] = ???

  def calcStats(
                 inData: Map[String, DataFrame[LocalDate, String, Double]],
                 inWeights: DataFrame[LocalDate, String, Double],
                 slippageFactor: Double = 0.05d,
                 maxPeriods:Int = 252*3,
                 minPeriods:Int = 2
               ):  DataFrame[LocalDate, String, Double] =  {
    val relativeReturns = calcRelativeReturn(
      inData,
      inWeights,
      slippageFactor
    )
    val volatility = calcVolatilityAnnualized(relativeReturns, maxPeriods, minPeriods)
    val meanReturn = calcMeanReturnAnnualized(relativeReturns, maxPeriods, minPeriods)
    val sharpeRatio = calcSharpeRatio(relativeReturns,  maxPeriods, minPeriods)
    val equity = calcEquity(relativeReturns)
    val underwater = calcUnderwater(equity)
    val maxDrawdown = calcMaxDrawdown(underwater)
    val bias = calcBias(inWeights)
    val instruments = calcInstruments(inWeights)
    val avgTurnover = calcAvgTurnover(inData(data.fields.open), inWeights, equity, maxPeriods, minPeriods)

    DataFrame.fromSeriesColumns(
      fields.relativeReturn -> relativeReturns,
      fields.volatility -> volatility.align(relativeReturns, Align.right),
      fields.sharpeRatio -> sharpeRatio.align(relativeReturns, Align.right),
      fields.equity -> equity.align(relativeReturns, Align.right),
      fields.underwater -> underwater.align(relativeReturns, Align.right),
      fields.maxDrawdown -> maxDrawdown.align(relativeReturns, Align.right),
      fields.bias -> bias.align(relativeReturns, Align.right),
      fields.instruments -> instruments.align(relativeReturns, Align.right),
      fields.avgTurnover -> avgTurnover.align(relativeReturns, Align.right)
    )
  }

  object fields {
    val relativeReturn = "relativeReturn"
    val volatility = "volatility"
    val sharpeRatio = "sharpeRatio"
    val equity = "equity"
    val underwater = "underwater"
    val maxDrawdown = "maxDrawdown"
    val bias = "bias"
    val instruments = "instruments"
    val avgTurnover = "avgTurnover"

    val values: IndexedSeq[String] = IndexedSeq(
      relativeReturn, volatility, sharpeRatio, equity, underwater, maxDrawdown, bias, instruments, avgTurnover)
  }
}
