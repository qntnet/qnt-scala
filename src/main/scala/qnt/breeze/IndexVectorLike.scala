package qnt.breeze

import breeze.linalg.VectorLike

import scala.reflect.ClassTag

trait IndexVectorLike[V] extends breeze.linalg.Vector[V] with VectorLike[V, IndexVectorLike[V]] {

  // TODO unique ?
  def ordered: Boolean

  def reversed: Boolean

  override def activeSize: Int = length

  override def activeIterator: Iterator[(Int, V)] = iterator

  override def activeValuesIterator: Iterator[V] = valuesIterator

  override def activeKeysIterator: Iterator[Int] = keysIterator

  override def repr: IndexVectorLike[V] = this

  override def toString: String = {
    valuesIterator.mkString(s"IndexVector(ordered=$ordered, reversed=$reversed, data=[", ", ", "])")
  }

  override def copy: IndexVector[V]

  def merge(other: IndexVectorLike[V])(implicit ord: Ordering[V], tag: ClassTag[V]): IndexVectorLike[V] = {
    var vals = Array.concat(toArray, other.toArray)
    vals = vals.distinct
    if (ordered) {
      vals = vals.sorted(ord)
      if (reversed) {
        vals = vals.reverse
      }
    }
    IndexVector[V](vals, ordered, reversed)
  }

  def indexOfUnexact(value: V)(implicit ord: Ordering[V]): Option[(Int, Int)] = {
    var exact = indexOfExact(value)
    if (exact.isDefined) {
      Some((exact.get, exact.get))
    } else {
      indexOfBinarySearch(value)
    }
  }

  def indexOfExact(value: V): Option[Int]

  def indexOfBinarySearch(value: V)(implicit ord: Ordering[V]): Option[(Int, Int)] = {
    if (!ordered) {
      return None
    }

    var leftIdx = 0
    var rightIdx = this.length - 1

    if (leftIdx > rightIdx) {
      return None
    }

    var leftVal: V = apply(leftIdx)
    var rightVal: V = apply(rightIdx)

    if (leftVal == value) {
      return Some((leftIdx, leftIdx))
    }

    if (rightVal == value) {
      return Some((rightIdx, rightIdx))
    }

    if (ord.lt(rightVal, value) ^ reversed) {
      return None
    }

    if (ord.gt(leftVal, value) ^ reversed) {
      return None
    }

    while (rightIdx - leftIdx > 1) {
      val midIdx = (rightIdx + leftIdx) / 2
      val midVal = apply(midIdx)
      if (midVal == value) {
        return Some((midIdx, midIdx))
      } else if (ord.lt(midVal, value) ^ reversed) {
        leftIdx = midIdx
        leftVal = midVal
      } else if (ord.gt(midVal, value) ^ reversed) {
        rightIdx = midIdx
        rightVal = midVal
      }
    }
    Some((leftIdx, rightIdx))
  }

  def sliceMask(mask: breeze.linalg.Vector[Boolean]): SliceIndexVector[V] = sliceMask(mask.valuesIterator)
  def sliceMask(mask: Boolean*): SliceIndexVector[V] = sliceMask(mask.iterator)
  def sliceMask(mask: Iterable[Boolean]): SliceIndexVector[V] = sliceMask(mask.iterator)
  def sliceMask(mask: Iterator[Boolean]): SliceIndexVector[V] = sliceSeq(mask.zipWithIndex.filter(_._1).map(_._2))

  def sliceSeq(idx: breeze.linalg.Vector[Int]): SliceIndexVector[V] = sliceSeq(idx.valuesIterator)
  def sliceSeq(idx: Int*): SliceIndexVector[V] = sliceSeq(idx.iterator)
  def sliceSeq(idx: Iterable[Int]): SliceIndexVector[V] = sliceSeq(idx.iterator)
  def sliceSeq(idx: Iterator[Int]): SliceIndexVector[V]

  def sliceRange(start: Int, end: Int, step: Int, left: Boolean, right: Boolean, round: Boolean)
    : SliceIndexVector[V] = sliceSeq(RoundArrayRange(length, start, end, step, left, right, round))

  def loc(v: V): Option[Int] = indexOfExact(v)

  def sliceLoc(v: breeze.linalg.Vector[V]): SliceIndexVector[V] = sliceLoc(v.valuesIterator)
  def sliceLoc(v: V*):SliceIndexVector[V] = sliceLoc(v.iterator)
  def sliceLoc(v: Iterable[V]): SliceIndexVector[V] = sliceLoc(v.iterator)
  def sliceLoc(v: Iterator[V]): SliceIndexVector[V] = {
    var idxo = v.map(indexOfExact).filter(_.isDefined).map(_.get)
    sliceSeq(idxo)
  }

  def sliceLocRange(start: V, end: V, step: Int = 1,
                    left: Boolean = true, right: Boolean = true, round: Boolean = true)
                   (implicit ord: Ordering[V], tag: ClassTag[V]): SliceIndexVector[V] = {
      val startIdx = indexOfUnexact(start)(ord)
      val endIdx = indexOfUnexact(end)(ord)
      if(startIdx.isEmpty || endIdx.isEmpty) {
        IndexVectorLike.empty[V].sliceSeq(Seq())
      } else {
        sliceRange(
          if(step > 0) startIdx.get._2 else startIdx.get._1,
          if(step > 0) endIdx.get._1 else endIdx.get._2,
          step,
          left, right, round
        )
      }
  }
}

object IndexVectorLike {
  def empty[V](implicit ord: Ordering[V], tag: ClassTag[V])
  = new IndexVector[V](Array[V](), true, false)(ord, tag)
}
