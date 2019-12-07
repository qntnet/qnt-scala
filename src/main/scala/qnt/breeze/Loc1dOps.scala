package qnt.breeze

trait Loc1dOps[K, V, S] {
  def at(v: K): Option[V]
  def loc(vals: breeze.linalg.Vector[K]): S = loc(vals.valuesIterator)
  def loc(vals: K*):S = loc(vals.iterator)
  def loc(vals: IterableOnce[K]): S
  def loc(start: K, end: K, step: Int = 1, keepStart: Boolean = true, keepEnd: Boolean = true, round: Boolean = true): S
}
