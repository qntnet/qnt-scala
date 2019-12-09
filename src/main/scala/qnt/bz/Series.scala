package qnt.bz

import breeze.linalg.{DenseVector, Tensor, TensorLike, Vector}

import scala.reflect.ClassTag

class Series[I, @specialized(Double, Int, Float, Long) V]
(val idx: IndexVector[I], val data: Vector[V])(implicit iTag:ClassTag[I], vTag: ClassTag[V])
  extends Tensor[I, V] with TensorLike[I, V, Series[I, V]]
    with Slice1dOps[I, Series[I, V]]
{

  if (idx.size != data.length) {
    throw new IllegalArgumentException("index.length != values.length")
  }

  if (!idx.unique) {
    throw new IllegalArgumentException("index must be unique")
  }

  override def apply(i: I): V = data(idx.indexOfExactUnsafe(i))

  override def update(i: I, v: V): Unit = data(idx.indexOfExactUnsafe(i)) = v

  override def size: Int = data.size

  override def activeSize: Int = idx.size

  override def keySet: Set[I] = idx.toSet

  override def keysIterator: Iterator[I] = keySet.iterator

  override def activeKeysIterator: Iterator[I] = keysIterator

  override def iterator: Iterator[(I, V)] = idx.valuesIterator.zip(data.valuesIterator)

  override def activeIterator: Iterator[(I, V)] = iterator

  override def valuesIterator: Iterator[V] = data.valuesIterator

  override def activeValuesIterator: Iterator[V] = valuesIterator

  override def repr: Series[I, V] = this

  override def loc(vals: IndexedSeq[I]): Series[I, V] = {
    val x = idx.loc(vals)
    Series[I, V](x, data(x.slices))
  }

  override def loc(start: I, end: I, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
  : Series[I, V] = {
    val x = idx.loc(start, end, step, keepEnd, keepEnd, round)
    Series[I, V](x, data(x.slices))
  }

  override def iloc(vals: IndexedSeq[Int]): Series[I, V] = {
    val x = idx.iloc(vals)
    Series[I, V](x, data(x.slices))
  }

  def copy: Series[I, V] = Series[I, V](idx.copy, data.copy.asInstanceOf[DenseVector[V]])

  override def toString: String = toString(5, 5)

  def toString(head: Int, tail:Int, indexFormat:I=>String = _.toString, dataFormat: V=>String = _.toString): String = {
    val rows = Math.min(idx.size, head+tail+1)
    val output = Array.ofDim[String](rows+2, 2)
    output(0)(0) = s"Series[${iTag.runtimeClass.getSimpleName},${vTag.runtimeClass.getSimpleName}]:"
    output(0)(1) = ""
    output(1)(0) = s"index"
    output(1)(1) = s"value"
    for(i <- 0 until rows) {
      val j = if (i < head) i else (idx.size - rows + i)
      output(2+i)(0) = indexFormat(idx(j))
      output(2+i)(1) = dataFormat(data(j))
      if (rows < idx.size && i == head) {
        output(2+i)(0) = "..."
        output(2+i)(1) = "..."
      }
    }
    // adjust column size
    for(c <- 0 to 1) {
      var size = 0
      for(r <- 1 until output.length) {
        size = Math.max(size, output(r)(c).length)
      }
      for(r <-  1 until output.length) {
        output(r)(c) = output(r)(c) + " " * (size - output(r)(c).length)
      }
    }
    output.map(r => r.mkString(" ")).mkString("\n")
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[Series[I, V]]

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
}

object Series {

    def apply[I, @specialized(Double, Int, Float, Long) V](index:  IndexVector[I], values: Vector[V])
                                                          (implicit iTag: ClassTag[I], vTag: ClassTag[V])
      = new Series[I,V](index, values)

}

