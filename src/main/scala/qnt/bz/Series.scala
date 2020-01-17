package qnt.bz

import breeze.linalg.{DenseVector, Vector}
import breeze.math.Semiring
import qnt.bz.Align._

import scala.reflect.ClassTag

class Series[I, @specialized(Double, Int, Float, Long) V]
(val idx: IndexVector[I], val data: Vector[V])(implicit iTag:ClassTag[I], vTag: ClassTag[V], vSem: Semiring[V])
  extends scala.collection.Map[I, V]
    with Slice1dOps[I, Series[I, V]] {

  if (idx.size != data.length) {
    throw new IllegalArgumentException("index.length != values.length")
  }

  if (!idx.unique) {
    throw new IllegalArgumentException("index must be unique")
  }

  override def knownSize = idx.length

  override def apply(i: I): V = data(idx.hashIndexOfUnsafe(i))

  def update(i: I, v: V): Unit = data(idx.hashIndexOfUnsafe(i)) = v

  override def loc(vals: IndexedSeq[I]): Series[I, V] = {
    val x = idx.loc(vals)
    Series[I, V](x, data(x.slices))
  }

  override def locRange(start: I, end: I, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
  : Series[I, V] = {
    val x = idx.locRange(start, end, step, keepEnd, keepEnd, round)
    Series[I, V](x, data(x.slices))
  }

  override def iloc(vals: IndexedSeq[Int]): Series[I, V] = {
    val x = idx.iloc(vals)
    Series[I, V](x, data(x.slices))
  }

  def copy: Series[I, V] = Series[I, V](idx.copy, data.copy.asInstanceOf[DenseVector[V]])

  override def toString: String = toString(5, 5)

  def toString(head: Int, tail: Int, indexFormat: I => String = _.toString, dataFormat: V => String = _.toString): String = {
    val rows = Math.min(idx.size, head + tail + 1)
    val output = Array.ofDim[String](rows + 2, 2)
    output(0)(0) = s"Series[${iTag.runtimeClass.getSimpleName},${vTag.runtimeClass.getSimpleName}]:"
    output(0)(1) = ""
    output(1)(0) = s"index"
    output(1)(1) = s"value"
    for (i <- 0 until rows) {
      val j = if (i < head) i else (idx.size - rows + i)
      output(2 + i)(0) = indexFormat(idx(j))
      output(2 + i)(1) = dataFormat(data(j))
      if (rows < idx.size && i == head) {
        output(2 + i)(0) = "..."
        output(2 + i)(1) = "..."
      }
    }
    // adjust column size
    for (c <- 0 to 1) {
      var size = 0
      for (r <- 1 until output.length) {
        size = Math.max(size, output(r)(c).length)
      }
      for (r <- 1 until output.length) {
        output(r)(c) = output(r)(c) + " " * (size - output(r)(c).length)
      }
    }
    output.map(r => r.mkString(" ")).mkString("\n")
  }

  override def equals(other: Any): Boolean = other match {
    case that: Series[I, V] =>
      (that canEqual this) &&
        idx == that.idx &&
        data == that.data
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), idx, data)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def align(right: Series[I, V], align: AlignType)
  : Series[I, V] = {
    val left = this
    var idx = left.idx.align(right.idx, align)
    this.align(idx)
  }

  def align(idx: IndexVector[I]): Series[I, V] = {
    val result = {
      if (this.idx == idx) this
      else if (idx.forall(i => this.idx.contains(i))) Series[I, V](idx, this.data(this.idx.loc(idx.toIndexedSeq).slices))
      else {
        var r = Series(idx, new DenseVector[V](idx.size))
        var ii = idx.intersect(this.idx)
        for (k <- ii.toIndexedSeq) {
          r(k) = this (k)
        }
        r
      }
    }
    result
  }

  def fillLike(fillValue: V): Series[I, V] = Series.fill(idx, fillValue)

  override def get(key: I): Option[V] = idx.hashIndexOf(key).map(i => data(i))

  override def iterator: Iterator[(I, V)] = idx.iterator.zip(data.valuesIterator)

  override def -(key: I): collection.Map[I, V] = ???

  override def -(key1: I, key2: I, keys: I*): collection.Map[I, V] = ???

  def shift(period:Int, fillValue: V): Series[I, V]  = {
    val result = copy
    if(period > 0) {
      for(i <- idx.indices) {
        if(i < period) {
          result.data.update(i, fillValue)
        } else {
          result.data.update(i, this.data(i - period))
        }
      }
    } else if (period < 0) {
      for(i <- idx.indices) {
        if(i > idx.indices.last - period) {
          result.data.update(i, fillValue)
        } else {
          result.data.update(i, this.data(i + period))
        }
      }
    }
    result
  }
}

object Series {

  def apply[I, @specialized(Double, Int, Float, Long) V](index:  IndexVector[I], values: Vector[V])
                                                (implicit iTag: ClassTag[I], vTag: ClassTag[V], vSem: Semiring[V])
    = new Series[I,V](index, values)

  def fill[I:ClassTag, @specialized(Double, Int, Float, Long) V:ClassTag:Semiring](index:  IndexVector[I], value: V)
  = new Series[I,V](index, DenseVector.fill(index.size, value))

}

