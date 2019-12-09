package qnt.bz

import breeze.linalg.{Tensor, TensorLike}

import scala.reflect.ClassTag

abstract class IndexVector[V]
() (implicit val ord: Ordering[V], val tag: ClassTag[V])
  extends Tensor[Int, V]
  with TensorLike[Int, V, IndexVector[V]]
  with Slice1dOps[V, SliceIndexVector[V]]
{

  def unique: Boolean
  def ordered: Boolean
  def reversed: Boolean

  override def activeSize: Int = size
  override def activeIterator: Iterator[(Int, V)] = iterator
  override def activeValuesIterator: Iterator[V] = valuesIterator
  override def activeKeysIterator: Iterator[Int] = keysIterator
  override def repr: IndexVector[V] = this
  override def iterator: Iterator[(Int, V)] = toIndexedSeq.indices.iterator.zip(toIndexedSeq)
  override def valuesIterator: Iterator[V] = toIndexedSeq.iterator
  override def keysIterator: Iterator[Int] = keySet.iterator
  def toArray: Array[V] = toIndexedSeq.toArray

  object keySet extends Set[Int] {
    override def incl(elem: Int): Set[Int] = Set() ++ iterator + elem

    override def excl(elem: Int): Set[Int] = Set() ++ iterator - elem

    override def contains(elem: Int): Boolean = elem >= 0 && elem < size

    override def iterator: Iterator[Int] = toIndexedSeq.iterator
  }

  object toIndexedSeq extends IndexedSeq[V] {
    override def apply(i: Int): V = IndexVector.this.apply(i)

    override def length: Int = IndexVector.this.size
  }

  def toSet: Set[V] = if(unique) valueSet else throw new IllegalArgumentException("not unique")

  private object valueSet extends Set[V] {

    override def incl(elem: V): Set[V] = Set() ++ iterator + elem

    override def excl(elem: V): Set[V] = Set() ++ iterator - elem

    override def contains(elem: V): Boolean = contains(elem)

    override def iterator: Iterator[V] = valuesIterator
  }

  override def toString: String = toString(5, 5)

  def toString(head: Int, tail:Int, format:V=>String = _.toString): String = {
    val rows = Math.min(size, head+tail+1)
    val output = Array.ofDim[String](rows+1)
    output(0) = s"Index[${tag.runtimeClass.getSimpleName}]:"
    for(i <- 0 until rows) {
      val j = if (i < head) i else (size - rows + i)
      output(1+i) = format(apply(j))
      if (rows < size && i == head) {
        output(1+i) = "..."
      }
    }
    output.mkString("\n")
  }

  def copy: DataIndexVector[V] = DataIndexVector[V](toArray, unique, ordered, reversed)

  def indexOfExact(value: V): Option[Int]

  def indexOfExactUnsafe(value: V): Int

  // TODO tests
  def indexOfBinarySearch(value: V): BinarySearchResult = {
    if (!ordered) {
      throw new IllegalStateException("unordered")
    }

    if(size < 1) {
      return BinarySearchResult(false,false)
    }

    var leftLeftIdx = 0
    var rightRightIdx = this.size - 1

    var leftVal: V = apply(leftLeftIdx)
    var rightVal: V = apply(rightRightIdx)

    @inline
    def findSameRightIdx(leftIdx: Int, leftVal: V) = {
      var rightIdx = leftIdx
      if (!unique) {
        do rightIdx += 1
        while (rightIdx < size && apply(rightIdx) == leftVal)
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
      return BinarySearchResult(true, false, leftLeftIdx, leftRightIdx)
    }

    if (rightVal == value) {
      return BinarySearchResult(true, false, leftLeftIdx, rightLeftIdx)
    }

    if (ord.lt(rightVal, value) ^ reversed) {
      return BinarySearchResult(false, false)
    }

    if (ord.gt(leftVal, value) ^ reversed) {
      return BinarySearchResult(false, false)
    }

    while (rightLeftIdx - leftRightIdx > 0) {
      val midIdx = (rightLeftIdx + leftRightIdx) / 2
      val midVal = apply(midIdx)

      var midLeftIdx = findSameLeftIdx(midIdx, midVal)
      var midRightIdx = findSameRightIdx(midIdx, midVal)

      if (midVal == value) {
        return BinarySearchResult(true, false, midLeftIdx, midRightIdx)
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
    BinarySearchResult(false, true, leftRightIdx, rightLeftIdx)
  }

  case class BinarySearchResult(
     foundValue: Boolean,
     foundRange: Boolean,
     start:Int = -1,
     end:Int = -1
   ){
    def found:Boolean = foundValue || foundRange
    def notFound:Boolean = !found
  }

  override def loc(vals: IndexedSeq[V]): SliceIndexVector[V] = {
    iloc(vals.map(indexOfExact).filter(_.isDefined).map(_.get))
  }

  override def loc(start: V, end: V, step: Int = 1, keepStart: Boolean = true, keepEnd: Boolean = true,
                   round: Boolean = true): SliceIndexVector[V] = {
      val startIdx = indexOfBinarySearch(start)
      val endIdx = indexOfBinarySearch(end)
      if(startIdx.notFound || endIdx.notFound) {
        IndexVector.empty[V].iloc()
      } else {
        iloc(
          if(step > 0)
            if(!unique && keepStart && startIdx.foundValue) startIdx.start else startIdx.end
          else
            if(!unique && keepEnd && startIdx.foundValue) startIdx.end else startIdx.start
          ,
          if(step > 0)
            if(!unique && keepEnd && endIdx.foundValue) endIdx.end else endIdx.start
          else
            if(!unique && keepStart && endIdx.foundValue) endIdx.start else endIdx.end
          ,
          step,
          keepStart, keepEnd, round
        )
      }
  }

  override def iloc(idx: IndexedSeq[Int]): SliceIndexVector[V] = SliceIndexVector[V](this, idx)

  def contains(v: V): Boolean

  def intersect(another: IndexVector[V]): SliceIndexVector[V]
  = iloc(iterator.filter(e => another.contains(e._2)).map(_._1).toIndexedSeq)


  def canEqual(other: Any): Boolean = other.isInstanceOf[IndexVector[V]]

  override def equals(other: Any): Boolean = other match {
    case that: IndexVector[V] =>
      (that canEqual this) &&
        ord == that.ord &&
        tag == that.tag &&
        unique == that.unique &&
        ordered == that.ordered &&
        reversed == that.reversed &&
        valuesIterator.zip(that.valuesIterator).forall(e=>e._1 == e._2)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), ord, tag)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object IndexVector {
  def empty[V](implicit ord: Ordering[V], tag: ClassTag[V])
   = new DataIndexVector[V](Array.empty[V],  true,true, false)(ord, tag)

  def combine[V]
  (vectors: Seq[IndexVector[V]]): DataIndexVector[V] = {
    var first = vectors(0)
    var vals = vectors.flatMap(_.valuesIterator)

    if(first.unique) {
      vals = vals.distinct
    }
    if (first.ordered) {
      vals = vals.sorted(vectors(0).ord)
      if (first.reversed) {
        vals = vals.reverse
      }
    }
    DataIndexVector[V](vals.toArray(first.tag), first.unique, first.ordered, first.reversed)(first.ord, first.tag)
  }
}
