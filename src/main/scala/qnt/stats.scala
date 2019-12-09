package qnt

//import org.saddle.{Series, Frame}

object stats {
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
