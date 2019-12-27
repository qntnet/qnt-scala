package qnt

import java.time.LocalDate

import qnt.bz.{Align, DataFrame, Series}

//import org.saddle.{Series, Frame}

object stats {

  private def calcTr(
                  high: DataFrame[LocalDate, String, Double],
                  low: DataFrame[LocalDate, String, Double],
                  close: DataFrame[LocalDate, String, Double]
                ): DataFrame[LocalDate, String, Double] = {
    val res = close.fillLike(Double.NaN)
    res.data.foreachKey(e => {
      val l = low.data(e)
      val h = high.data(e)
      val c = close.data(e)
      val d1 = h - l
      val d2 = h - c
      val d3 = c - l
      val r = math.max(math.max(d1,d2),d3)
      res.data(e) = r
    })
    res
  }

  private def calcSma(d: DataFrame[LocalDate, String, Double], period: Int): DataFrame[LocalDate, String, Double] = {
    // sma without nan correction
    val res = d.fillLike(Double.NaN)
    for(c <- d.colIdx.indices; r <- period-1 to d.rowIdx.indices.end) { //TODO transpose to speed up
      var sum = 0d
      for(i <- 0 until period) {
        sum = sum + d.data(r-i, c)
      }
      res.data.update(r, c, sum / period)
    }
    res
  }

  private def calcSlippage
  (
    high: DataFrame[LocalDate, String, Double],
    low: DataFrame[LocalDate, String, Double],
    close: DataFrame[LocalDate, String, Double],
    period: Int = 14,
    fract: Double = 0.05d
  ): DataFrame[LocalDate, String, Double] = {
    var res = calcTr(high, low, close);
    res = calcSma(res, period)
    res.data := res.data * fract
    res
  }

  def calcRelativeReturns(
                           inOpen: DataFrame[LocalDate, String, Double],
                           inHigh: DataFrame[LocalDate, String, Double],
                           inLow: DataFrame[LocalDate, String, Double],
                           inClose: DataFrame[LocalDate, String, Double],
                           inLiquid: DataFrame[LocalDate, String, Double],
                           inWeights: DataFrame[LocalDate, String, Double],
                           slippageFactor: Double = 0.05d
                         ): Series[LocalDate, Double] = {

    val weights = inWeights.align(inClose, Align.right, Double.NaN)
    val open = inOpen.align(inClose, Align.right, Double.NaN)
    val high = inHigh.align(inClose, Align.right, Double.NaN)
    val low = inLow.align(inClose, Align.right, Double.NaN)
    val close = inClose
    val liquid = inLiquid.align(inClose, Align.right, Double.NaN)
    val slippage = calcSlippage(high, low, close, fract=slippageFactor)

    val N = weights.fillLike(0)
    val equityBeforeBuy = weights.fillLike(0)
    val equityAfterBuy = weights.fillLike(0)
    val equityTonight = weights.fillLike(0)

    val unlocked = Series.fill(weights.colIdx, 0)

    for(ti <- weights.rowIdx.indices) {


    }


    null
  }

//
//  def calcRelativeReturns(
//                           inOpen:Frame[LocalDate, String, Double],
//                           inClose:Frame[LocalDate, String, Double],
//                           inLiquid:Frame[LocalDate, String, Double],
//                           inWeights:Frame[LocalDate, String, Double]
//                         ):Series[LocalDate, Double] = {
//    // arrangement
//    var (weights, _) = inWeights.align(inOpen)
//    val (open, _) = inOpen.align(weights)
//    val (close, _) = inClose.align(weights)
//    val (liquid, _) = inLiquid.align(weights)
//


    /*
    N = np.zeros(WEIGHT.shape)  # shares count

    equity_before_buy = np.zeros([WEIGHT.shape[0]])
    equity_operable_before_buy = np.zeros([WEIGHT.shape[0]])
    equity_after_buy = np.zeros([WEIGHT.shape[0]])
    equity_tonight = np.zeros([WEIGHT.shape[0]])

    for t in range(0, WEIGHT.shape[0]):
        unlocked = UNLOCKED[t]  # available for trading
        locked = np.logical_not(unlocked)

        if t == 0:
            equity_before_buy[0] = 1
            N[0] = 0
        else:
            N[t] = N[t - 1]
            divs = np.nansum(N[t] * DIVS[t])
            equity_before_buy[t] = equity_after_buy[t - 1] + np.nansum((OPEN[t] - OPEN[t - 1]) * N[t]) + divs

        w_sum = np.nansum(abs(WEIGHT[t]))
        w_free_cash = max(1, w_sum) - w_sum
        w_unlocked = np.nansum(abs(WEIGHT[t, unlocked]))
        w_operable = w_unlocked + w_free_cash

        equity_operable_before_buy[t] = equity_before_buy[t] - np.nansum(OPEN[t, locked] * abs(N[t, locked]))

        if w_operable < EPS:
            equity_after_buy[t] = equity_before_buy[t]
        else:
            N[t, unlocked] = equity_operable_before_buy[t] * WEIGHT[t, unlocked] / (w_operable * OPEN[t, unlocked])
            dN = N[t, unlocked]
            if t > 0:
                dN = dN - N[t - 1, unlocked]
            S = np.nansum(SLIPPAGE[t, unlocked] * abs(dN))  # slippage for this step
            equity_after_buy[t] = equity_before_buy[t] - S

        equity_tonight[t] = equity_after_buy[t] + np.nansum((CLOSE[t] - OPEN[t]) * N[t])

    E = equity_tonight
    Ep = np.roll(E, 1)
    Ep[0] = 1
    RR = E / Ep - 1
    RR = np.where(np.isfinite(RR), RR, 0)

    null
  }
  */
}
