package qnt.breeze

import breeze.linalg.Vector

import scala.collection.{IterableOnce, mutable}
import scala.reflect.ClassTag

class IndexVector[V] (
  private val data: Array[V],
  val unique: Boolean,
  val ordered: Boolean,
  val reversed: Boolean,
) (implicit ord: Ordering[V], tag: ClassTag[V]) extends IndexVectorLike[V]  {

  private val valueToIdxMap = new mutable.HashMap[V,Int]()

  if(ordered) {
    for (i <- 0 to (data.length - 2))
      if (ord.gt(data(i), data(i + 1)) ^ reversed) {
        throw new IllegalArgumentException(s"ordering violation idx1=$i idx2=${i + 1}")
      }
  }
  for (i <- data.indices) {
    var v = data(i)
    if(unique && valueToIdxMap.contains(v)) {
      throw new IllegalArgumentException(s"duplicate idx=$i val=$v")
    }
    valueToIdxMap(v) = i
  }

  override def copy: IndexVector[V] = {
    IndexVector[V](toArray, unique, ordered, reversed)
  }

  override def update(i: Int, v: V): Unit = {
    if (data(i) == v) {
      return
    }
    if (unique && data.contains(v)) {
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
  }

  override def length: Int = data.length

  override def apply(i: Int): V = data(i)

  override def indexOfExact(v:V): Option[Int] =
    if(unique) valueToIdxMap.get(v) else throw new IllegalStateException("not unique")

  override def sliceIdx(idx: IterableOnce[Int]): SliceIndexVector[V] =
    new SliceIndexVector[V](this, idx.iterator.toIndexedSeq)

}

object IndexVector {

  def apply[V](data: IterableOnce[V], unique: Boolean, ordered: Boolean, reversed: Boolean)
              (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V] = {
    new IndexVector[V](data.iterator.toArray, unique, ordered, reversed)(ord, tag)
  }

  def apply[V](data: IterableOnce[V])(implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V] = {
    val arr = data.iterator.toArray
    val unique = arr.distinct.length == arr.length
    val ascending = arr.indices.forall(i => i == arr.indices.end || ord.lt(arr(i), arr(i+1)))
    val descending = arr.indices.forall(i => i == arr.indices.end || ord.gt(arr(i), arr(i+1)))
    apply(arr, unique, ascending || descending, descending)
  }

  def apply[V](sliceVector: SliceIndexVector[V])
              (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V] = {
    val values = sliceVector.toArray
    val t = sliceVector.indexVector
    apply(values, t.unique, t.ordered, t.reversed)
  }

  def apply[V](vector: Vector[V], unique: Boolean, ordered: Boolean, reversed: Boolean)
              (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V]
    = apply(vector.valuesIterator, unique, ordered, reversed)

  def apply[V](vector: Vector[V]) (implicit ord: Ordering[V], tag: ClassTag[V]) : IndexVector[V]
    = apply(vector.valuesIterator)

}

