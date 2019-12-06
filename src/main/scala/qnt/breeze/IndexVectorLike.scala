package qnt.breeze

import breeze.linalg.VectorLike

import scala.reflect.ClassTag

trait IndexVectorLike[V] extends breeze.linalg.Vector[V] with VectorLike[V, IndexVectorLike[V]] {

  def unique: Boolean
  def ordered: Boolean
  def reversed: Boolean

  override def activeSize: Int = length
  override def activeIterator: Iterator[(Int, V)] = iterator
  override def activeValuesIterator: Iterator[V] = valuesIterator
  override def activeKeysIterator: Iterator[Int] = keysIterator
  override def repr: IndexVectorLike[V] = this
  override def toString: String = {
    valuesIterator.mkString(s"Index(unique=$unique,ordered=$ordered,reversed=$reversed,data=[", ", ", "])")
  }

  override def copy: IndexVector[V]

  def merge(other: IndexVectorLike[V])(implicit ord: Ordering[V], tag: ClassTag[V]): IndexVectorLike[V] = {
    var vals = Array.concat(toArray, other.toArray)
    if(unique) {
      vals = vals.distinct
    }
    if (ordered) {
      vals = vals.sorted(ord)
      if (reversed) {
        vals = vals.reverse
      }
    }
    IndexVector[V](vals, unique, ordered, reversed)
  }

  def indexOfUnexact(value: V)(implicit ord: Ordering[V]): Option[(Int, Int)] = {
    var exact = if(unique) indexOfExact(value) else None
    if (exact.isDefined) {
      Some((exact.get, exact.get))
    } else {
      indexOfBinarySearch(value)
    }
  }

  def indexOfExact(value: V): Option[Int]

  // returns range of indexes
  //   or index of first element that less and index of first element that great
  //   or None
  // TODO tests
  def indexOfBinarySearch(value: V)(implicit ord: Ordering[V]): Option[(Int, Int)] = {
    if (!ordered || length < 1) {
      return None
    }

    var leftLeftIdx = 0
    var rightRightIdx = this.length - 1

    var leftVal: V = apply(leftLeftIdx)
    var rightVal: V = apply(rightRightIdx)

    @inline
    def findSameRightIdx(leftIdx: Int, leftVal: V) = {
      var rightIdx = leftIdx
      if (!unique) {
        do rightIdx += 1
        while (rightIdx < length && apply(rightIdx) == leftVal)
        rightIdx -= 1
      }
      rightIdx
    }

    @inline
    def findSameLeftIdx(rightIdx: Int, rightVal: V) = {
      var leftIdx = rightIdx
      if (!unique) {
        do leftIdx -= 1
        while (rightIdx > 0 && apply(leftIdx) == rightVal)
        leftIdx += 1
      }
      leftIdx
    }

    var leftRightIdx = findSameRightIdx(leftLeftIdx, leftVal)
    var rightLeftIdx = findSameLeftIdx(rightRightIdx, rightVal)

    if (leftVal == value) {
      return Some((leftLeftIdx, leftRightIdx))
    }

    if (rightVal == value) {
      return Some((leftLeftIdx, rightLeftIdx))
    }

    if (ord.lt(rightVal, value) ^ reversed) {
      return None
    }

    if (ord.gt(leftVal, value) ^ reversed) {
      return None
    }

    while (rightLeftIdx - leftRightIdx > 0) {
      val midIdx = (rightLeftIdx + leftRightIdx) / 2
      val midVal = apply(midIdx)

      var midLeftIdx = findSameLeftIdx(midIdx, midVal)
      var midRightIdx = findSameRightIdx(midIdx, midVal)

      if (midVal == value) {
        return Some((midLeftIdx, midRightIdx))
      } else if (ord.lt(midVal, value) ^ reversed) {
        leftLeftIdx = midLeftIdx
        leftRightIdx = midRightIdx
        leftVal = midVal
      } else if (ord.gt(midVal, value) ^ reversed) {
        rightRightIdx = midRightIdx
        rightLeftIdx = midLeftIdx
        rightVal = midVal
      }
    }
    Some((leftRightIdx, rightLeftIdx))
  }

  def sliceMask(mask: breeze.linalg.Vector[Boolean]): SliceIndexVector[V] = sliceMask(mask.valuesIterator)
  def sliceMask(mask: Boolean*): SliceIndexVector[V] = sliceMask(mask)
  def sliceMask(mask: IterableOnce[Boolean]): SliceIndexVector[V] =
    sliceIdx(mask.iterator.zipWithIndex.filter(_._1).map(_._2))

  def sliceIdx(idx: breeze.linalg.Vector[Int]): SliceIndexVector[V] = sliceIdx(idx.valuesIterator)
  def sliceIdx(idx: Int*): SliceIndexVector[V] = sliceIdx(idx.iterator)
  def sliceIdx(idx: IterableOnce[Int]): SliceIndexVector[V]

  def sliceRange(start: Int, end: Int, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
    : SliceIndexVector[V] = sliceIdx(RoundArrayRange(length, start, end, step, keepStart, keepEnd, round))

  def loc(v: V): Option[Int] = indexOfExact(v)

  def sliceLoc(vals: breeze.linalg.Vector[V]): SliceIndexVector[V] = sliceLoc(vals.valuesIterator)
  def sliceLoc(vals: V*):SliceIndexVector[V] = sliceLoc(vals.iterator)
  def sliceLoc(vals: IterableOnce[V]): SliceIndexVector[V] = {
    sliceIdx(vals.iterator.map(indexOfExact).filter(_.isDefined).map(_.get))
  }

  def sliceLocRange(start: V, end: V, step: Int = 1,
                    keepStart: Boolean = true, keepEnd: Boolean = true, round: Boolean = true)
                   (implicit ord: Ordering[V], tag: ClassTag[V]): SliceIndexVector[V] = {
      val startIdx = indexOfUnexact(start)(ord)
      val endIdx = indexOfUnexact(end)(ord)
      if(startIdx.isEmpty || endIdx.isEmpty) {
        IndexVectorLike.empty[V].sliceIdx()
      } else {
        sliceRange(
          if(step > 0)
            if(!unique && keepStart && apply(startIdx.get._1) == start) startIdx.get._1 else startIdx.get._2
          else
            if(!unique && keepEnd && apply(startIdx.get._1) == start) startIdx.get._2 else startIdx.get._1
          ,
          if(step > 0)
            if(!unique && keepEnd && apply(endIdx.get._1) == end) startIdx.get._2 else startIdx.get._1
          else
            if(!unique && keepStart && apply(endIdx.get._1) == end) startIdx.get._1 else startIdx.get._2
          ,
          step,
          keepStart, keepEnd, round
        )
      }
  }
}

object IndexVectorLike {
  def empty[V](implicit ord: Ordering[V], tag: ClassTag[V])
  = new IndexVector[V](Array.empty[V],  true,true, false)(ord, tag)
}
