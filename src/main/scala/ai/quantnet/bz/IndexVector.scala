package ai.quantnet.bz

import ai.quantnet.bz.Align.AlignType

import scala.reflect.ClassTag

abstract class IndexVector[V] () (implicit val ord: Ordering[V], val tag: ClassTag[V])
  extends scala.collection.mutable.IndexedSeq[V]
  with Slice1dOps[V, SliceIndexVector[V]]
{

  def unique: Boolean
  def ordered: Boolean
  def descending: Boolean

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

  def copy: DataIndexVector[V] = DataIndexVector[V](this, unique, ordered, descending)

  override def indexOf[B >: V](elem: B): Int = {
    val v = elem.asInstanceOf[V]
    if(unique) {
      hashIndexOf(v).getOrElse(-1)
    } else {
      super.indexOf(v)
    }
  }

  def hashIndexOf(value: V): Option[Int]

  def hashIndexOfUnsafe(value: V): Int

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

    if (ord.lt(rightVal, value) ^ descending) {
      return BinarySearchResult(false, false)
    }

    if (ord.gt(leftVal, value) ^ descending) {
      return BinarySearchResult(false, false)
    }

    while (rightLeftIdx - leftRightIdx > 0) {
      val midIdx = (rightLeftIdx + leftRightIdx) / 2
      val midVal = apply(midIdx)

      var midLeftIdx = findSameLeftIdx(midIdx, midVal)
      var midRightIdx = findSameRightIdx(midIdx, midVal)

      if (midVal == value) {
        return BinarySearchResult(true, false, midLeftIdx, midRightIdx)
      } else if (ord.lt(midVal, value) ^ descending) {
        leftLeftIdx = midLeftIdx
        leftRightIdx = midRightIdx
        leftVal = midVal
      } else if (ord.gt(midVal, value) ^ descending) {
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
    iloc(vals.map(hashIndexOf).filter(_.isDefined).map(_.get))
  }

  override def locRange(start: V, end: V, step: Int = 1, keepStart: Boolean = true, keepEnd: Boolean = true,
                        round: Boolean = true): SliceIndexVector[V] = {
      val startIdx = indexOfBinarySearch(start)
      val endIdx = indexOfBinarySearch(end)
      if(startIdx.notFound || endIdx.notFound) {
        IndexVector.empty[V].iloc(Seq())
      } else {
        ilocRange(
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

  def intersect(anothers: IndexVector[V]*): SliceIndexVector[V] = intersect(anothers)

  def intersect(anothers: IterableOnce[IndexVector[V]]): SliceIndexVector[V]
  = iloc(this.iterator.zipWithIndex.filter(e => anothers.iterator.indexWhere(_.contains(e._1)) >= 0).map(_._2).toIndexedSeq)

  def combine(vectors: IndexVector[V]*): DataIndexVector[V] = combine(vectors)

  def combine(vectors: IterableOnce[IndexVector[V]]): DataIndexVector[V] = {
    var vals = this.iterator.concat(vectors.iterator.flatMap(_.iterator)).toSeq
    if(unique) {
      vals = vals.distinct
    }
    if (ordered) {
      vals = vals.sorted(this.ord)
      if (descending) {
        vals = vals.reverse
      }
    }
    DataIndexVector[V](vals.toArray(tag), unique, ordered, descending)(ord, tag)
  }

  def align(right: IndexVector[V], align: AlignType): IndexVector[V] = {
    val left = this
    align match {
      case Align.left => left
      case Align.right => right
      case Align.outer => left.combine(right)
      case Align.inner => left.intersect(right)
      case _ => throw new IllegalArgumentException("wrong align " + align);
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: IndexVector[V] =>
      (that canEqual this) &&
        ord == that.ord &&
        tag == that.tag &&
        unique == that.unique &&
        ordered == that.ordered &&
        descending == that.descending &&
        super.equals(other)
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), ord, tag)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  object asMap extends collection.Map[V, Int] {
    override def -(key: V): collection.Map[V, Int] = ???
    override def -(key1: V, key2: V, keys: V*): collection.Map[V, Int] = ???

    override def get(key: V): Option[Int] = hashIndexOf(key)
    override def iterator: Iterator[(V, Int)] = IndexVector.this.iterator.zipWithIndex
    override val knownSize = IndexVector.this.size
  }
}

object IndexVector {
  def empty[V](implicit ord: Ordering[V], tag: ClassTag[V])
   = new DataIndexVector[V](Array.empty[V],  true,true, false)(ord, tag)

}
