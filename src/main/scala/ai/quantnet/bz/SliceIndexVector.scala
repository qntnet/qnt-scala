package ai.quantnet.bz

import scala.reflect.ClassTag

class SliceIndexVector[V](
                           val source: IndexVector[V],
                           val slices: IndexedSeq[Int]
) (implicit ord: Ordering[V], tag: ClassTag[V])
  extends IndexVector[V] {

  override val unique: Boolean = source.unique && slices.distinct.length == slices.length

  override val length: Int = slices.length

  override val (ordered:Boolean, descending:Boolean) =
    if(!source.ordered) {
      val ascending = slices.indices.forall(i => i == slices.length - 1 || slices(i) < slices(i + 1))
      val descending = slices.indices.forall(i => i == slices.length - 1 || slices(i) > slices(i + 1))
      (ascending || descending, source.descending ^ descending)
    } else {
      (false, false)
    }

  private val sourceToLocalIdxMap = slices.zipWithIndex.toMap

  override def hashIndexOf(value: V): Option[Int] = {
    if(!unique) throw new IllegalStateException("not unique")
    source.hashIndexOf(value).map(sourceToLocalIdxMap)
  }

  override def hashIndexOfUnsafe(value: V): Int = sourceToLocalIdxMap(source.hashIndexOfUnsafe(value))

  override def apply(i: Int): V = source(slices(i))

  override def update(i: Int, v: V): Unit = source(slices(i))

  override def contains[A1 >: V](elem: A1): Boolean = {
    val v = elem.asInstanceOf[V]
    if (unique) source.contains(v) && sourceToLocalIdxMap.contains(source.hashIndexOfUnsafe(v))
    else if (ordered) indexOfBinarySearch(v).foundValue
    else iterator.contains(v)
  }

}

object SliceIndexVector {
  def apply[V](indexVector: IndexVector[V], slices: IndexedSeq[Int])
              (implicit ord: Ordering[V], tag: ClassTag[V]): SliceIndexVector[V]
    = new SliceIndexVector[V](indexVector, slices.iterator.toIndexedSeq)
}