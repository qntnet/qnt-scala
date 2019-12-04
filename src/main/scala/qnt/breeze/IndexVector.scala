package qnt.breeze

import breeze.linalg.{SliceVector, Vector}

import scala.collection.mutable
import scala.reflect.ClassTag

class IndexVector[V]
(
  private val data: Array[V],
  val ordered: Boolean,
  val reversed: Boolean,
)(implicit ord: Ordering[V], tag: ClassTag[V]) extends IndexVectorLike[V]  {

  private val valueToIdxMap = new mutable.HashMap[V,Int]()

  if(ordered) {
    for (i <- 0 to (data.length - 2))
      if (ord.gt(data(i), data(i + 1)) ^ reversed) {
        throw new IllegalArgumentException(s"ordering violation idx1=$i idx2=${i + 1}")
      }
  }
  for (i <- data.indices) {
    var v = data(i)
    if(valueToIdxMap.contains(v)) {
      throw new IllegalArgumentException(s"duplicate idx=$i val=$v")
    }
    valueToIdxMap(v) = i
  }

  override def copy: IndexVector[V] = {
    IndexVector[V](toArray, ordered, reversed)
  }

  override def update(i: Int, v: V): Unit = {
    if (data(i) == v) {
      return
    }
    if (data.contains(v)) {
      throw new IllegalStateException(s"duplicate $i")
    }
    if (ordered) {
      if (i > 0) {
        val prev = apply(i - 1)
        if (ord.lt(prev, v) ^ reversed) {
          throw new IllegalArgumentException(s"ordering violation (prev, cur) idx=$i")
        }
      }
      if (i < length - 1) {
        val nxt = apply(i + 1)
        if (ord.gt(v, nxt) ^ reversed) {
          throw new IllegalArgumentException(s"ordering violation (cur, nxt) idx=$i")
        }
      }
    }
    valueToIdxMap.remove(data(i))
    data(i) = v
    valueToIdxMap(v) = i

    SliceVector
  }

  override def length: Int = data.length

  override def apply(i: Int): V = data(i)

  override def indexOfExact(v:V): Option[Int] = valueToIdxMap.get(v)

  override def sliceSeq(idx: Iterator[Int]): SliceIndexVector[V] = new SliceIndexVector[V](this, idx.toIndexedSeq)


}

object IndexVector {

  def apply[V](data: Array[V], ordered: Boolean, reversed: Boolean)
              (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V] = {
    new IndexVector[V](data, ordered, reversed)(ord, tag)
  }

  def apply[V](sliceVector: SliceVector[Int, V])
              (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V] = {
    sliceVector.tensor match {
      case t: IndexVector[V] =>
        val values = sliceVector.toArray
        apply(values, t.ordered, t.reversed)
      case _ =>
        throw new IllegalArgumentException("tensor of slice is not IndexVector")
    }
  }

  def apply[V](vector: Vector[V], ordered: Boolean, reversed: Boolean)
              (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V] = {
    val values = vector.toArray
    apply(values, ordered, reversed)
  }
}

