package qnt.breeze

import scala.reflect.ClassTag

class SliceIndexVector[V](
                           val source: AbstractIndexVector[V],
                           val slices: IndexedSeq[Int]
) (implicit ord: Ordering[V], tag: ClassTag[V])
  extends AbstractIndexVector[V] {

  override val unique: Boolean = source.unique && slices.distinct.length == slices.length

  override val (ordered:Boolean, reversed:Boolean) =
    if(!source.ordered) {
      val ascending = slices.indices.forall(i => i == slices.length - 1 || slices(i) < slices(i + 1))
      val descending = slices.indices.forall(i => i == slices.length - 1 || slices(i) > slices(i + 1))
      (ascending || descending, source.reversed ^ descending)
    } else {
      (false, false)
    }

  private val sourceToLocalIdxMap = slices.zipWithIndex.toMap

  override def indexOfExact(value: V): Option[Int] = {
    if(!unique) throw new IllegalStateException("not unique")
    source.indexOfExact(value).map(sourceToLocalIdxMap)
  }

  override def indexOfExactUnsafe(value: V): Int = sourceToLocalIdxMap(source.indexOfExactUnsafe(value))

  override def length: Int = slices.length

  override def apply(i: Int): V = source(slices(i))

  override def update(i: Int, v: V): Unit = source(slices(i))

  override def contains(v: V): Boolean
  = if (unique) source.contains(v) && sourceToLocalIdxMap.contains(source.indexOfExactUnsafe(v))
  else if (ordered) indexOfBinarySearch(v).foundValue
  else valuesIterator.contains(v)

}

object SliceIndexVector {
  def apply[V](indexVector: AbstractIndexVector[V], slices: IterableOnce[Int])
              (implicit ord: Ordering[V], tag: ClassTag[V]): SliceIndexVector[V]
    = new SliceIndexVector[V](indexVector, slices.iterator.toIndexedSeq)
}