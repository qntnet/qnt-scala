package ai.quantnet.bz

import scala.collection.{IterableOnce, mutable}
import scala.reflect.ClassTag

class DataIndexVector[V](
                          private val data: Array[V],
                          val unique: Boolean,
                          val ordered: Boolean,
                          val descending: Boolean,
                    )(implicit ord: Ordering[V], tag: ClassTag[V]) extends IndexVector[V] {

  private val valueToIdxMap = new mutable.HashMap[V, Int]()

  if (ordered) {
    for (i <- 0 to (data.length - 2))
      if (ord.gt(data(i), data(i + 1)) ^ descending) {
        throw new IllegalArgumentException(s"ordering violation idx1=$i idx2=${i + 1}")
      }
  }
  for (i <- data.indices) {
    var v = data(i)
    if (unique && valueToIdxMap.contains(v)) {
      throw new IllegalArgumentException(s"duplicate idx=$i val=$v")
    }
    valueToIdxMap(v) = i
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
        if (ord.lt(prev, v) ^ descending) {
          throw new IllegalArgumentException(s"ordering violation (prev, cur) idx=$i")
        }
      }
      if (i < size - 1) {
        val nxt = apply(i + 1)
        if (ord.gt(v, nxt) ^ descending) {
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

  override def contains[A1 >: V](elem: A1): Boolean = {
    val v = elem.asInstanceOf[V]
    if (unique) valueToIdxMap.contains(v)
    else if (ordered) indexOfBinarySearch(v).foundValue
    else data.contains(v)
  }

  override def hashIndexOf(v: V): Option[Int] =
    if (unique) valueToIdxMap.get(v) else throw new IllegalStateException("not unique")

  override def hashIndexOfUnsafe(value: V): Int = valueToIdxMap.apply(value)

}

object DataIndexVector {

  def apply[V](data: IterableOnce[V], unique: Boolean, ordered: Boolean, reversed: Boolean)
              (implicit ord: Ordering[V], tag: ClassTag[V]): DataIndexVector[V] = {
    new DataIndexVector[V](data.iterator.toArray, unique, ordered, reversed)(ord, tag)
  }

  def apply[V](data: IterableOnce[V])(implicit ord: Ordering[V], tag: ClassTag[V]): DataIndexVector[V] = {
    val arr = data.iterator.toArray
    val unique = arr.distinct.length == arr.length
    val ascending = arr.indices.take(arr.length-1).forall(i => ord.lt(arr(i), arr(i + 1)))
    val descending = arr.indices.take(arr.length-1).forall(i => ord.gt(arr(i), arr(i + 1)))
    apply(arr, unique, ascending || descending, descending)
  }

}

