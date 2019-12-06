package qnt.breeze

import scala.reflect.ClassTag

class SliceIndexVector[V](
  val indexVector: IndexVector[V],
  val slices: IndexedSeq[Int]
) (implicit ord: Ordering[V], tag: ClassTag[V])
  extends IndexVectorLike[V] {

  override val unique: Boolean = indexVector.unique && slices.distinct.length == slices.length

  override val ordered: Boolean = indexVector.ordered &&
    (slices.indices.forall(i => i == slices.length - 1 || slices(i) < slices(i + 1))
      || slices.indices.forall(i => i == slices.length - 1 || slices(i) > slices(i + 1)))

  override val reversed: Boolean = indexVector.reversed ^
    slices.indices.forall(i => i == slices.indices.end || slices(i) > slices(i + 1))

  private val tensorToLocalIdxMap = slices.zipWithIndex.toMap

  override def indexOfExact(value: V): Option[Int] = {
    var origIdx = indexVector.indexOfExact(value)
    if (origIdx.isEmpty) origIdx else tensorToLocalIdxMap.get(origIdx.get)
  }

  override def copy: IndexVector[V] = IndexVector(toArray, unique, ordered, reversed)

  override def length: Int = slices.length

  override def apply(i: Int): V = indexVector(slices(i))

  override def update(i: Int, v: V): Unit = indexVector(slices(i))

  override def sliceIdx(idx: IterableOnce[Int]): SliceIndexVector[V]
    = new SliceIndexVector[V](indexVector, idx.iterator.map(i => slices(i)).toIndexedSeq)

}
