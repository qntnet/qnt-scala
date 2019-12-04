package qnt.breeze

import scala.reflect.ClassTag

class SliceIndexVector[V](
                             tensor: IndexVector[V],
                             slices: IndexedSeq[Int]
                           )
                         (implicit ord: Ordering[V], tag: ClassTag[V])
  extends IndexVectorLike[V] {

  override val ordered: Boolean = tensor.ordered && (
    slices.indices.forall(i => i == slices.length - 1 || slices(i) < slices(i + 1))
      ||
      slices.indices.forall(i => i == slices.length - 1 || slices(i) > slices(i + 1))
    )

  override val reversed: Boolean = tensor.reversed ^ slices.indices.forall(i => i == slices.length - 1 || slices(i) > slices(i + 1))

  private val tensorToLocalIdxMap = slices.zipWithIndex.toMap

  override def indexOfExact(value: V): Option[Int] = {
    var origIdx = tensor.indexOfExact(value)
    if (origIdx.isEmpty) origIdx else tensorToLocalIdxMap.get(origIdx.get)
  }

  override def copy: IndexVector[V] = IndexVector(toArray, ordered, reversed)

  override def length: Int = slices.length

  override def apply(i: Int): V = tensor(slices(i))

  override def update(i: Int, v: V): Unit = tensor(slices(i))

  override def sliceSeq(idx: Iterator[Int]): SliceIndexVector[V]
  = new SliceIndexVector[V](tensor, idx.map(i => slices(i)).toIndexedSeq)

}
