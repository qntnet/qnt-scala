package qnt.bz

trait Slice1dOps[K, S] {
  def size: Int

  def iloc(vals: Int*): S = iloc(vals.toIndexedSeq)
  def iloc(vals: IterableOnce[Int]): S = iloc(vals.iterator.toIndexedSeq)
  def iloc(vals: Iterable[Int]): S = iloc(vals.toIndexedSeq)
  def iloc(vals: IndexedSeq[Int]): S
  def ilocRange(start: Int, end: Int, step: Int = 1, keepStart: Boolean = true, keepEnd: Boolean = true, round: Boolean = true)
  : S = iloc(RoundArrayRange(size, start, end, step, keepStart, keepEnd, round))

  def mask(m: Boolean*): S = mask(m.toIndexedSeq)
  def mask(m: IterableOnce[Boolean]): S = {
    val idx = m.iterator.zipWithIndex.filter(_._1).map(_._2).toArray
    iloc(idx)
  }

  def loc(vals: K*):S = loc(vals.toIndexedSeq)
  def loc(vals: IterableOnce[K]): S = loc(vals.iterator.toIndexedSeq)
  def loc(vals: Iterable[K]): S = loc(vals.iterator.toIndexedSeq)
  def loc(vals: IndexedSeq[K]): S
  def locRange(start: K, end: K, step: Int = 1, keepStart: Boolean = true, keepEnd: Boolean = true, round: Boolean = true): S
}
