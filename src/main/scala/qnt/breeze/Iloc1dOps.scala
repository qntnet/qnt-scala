package qnt.breeze

trait Iloc1dOps[K, V, S] {
  def size: Int

  def iat(i: Int): V

  def iloc(vals: breeze.linalg.Vector[Int]): S = iloc(vals.valuesIterator)

  def iloc(vals: Int*): S = iloc(vals.iterator)

  def iloc(vals: IterableOnce[Int]): S

  def iloc(start: Int, end: Int, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
  : S = iloc(RoundArrayRange(size, start, end, step, keepStart, keepEnd, round))

  def mask(m: breeze.linalg.Vector[Boolean]): S = mask(m.valuesIterator)

  def mask(m: Boolean*): S = mask(m)

  def mask(m: IterableOnce[Boolean]): S = {
    val idx = m.iterator.zipWithIndex.filter(_._1).map(_._2).toArray
    iloc(idx)
  }
}
