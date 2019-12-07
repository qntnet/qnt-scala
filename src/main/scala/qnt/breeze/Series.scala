package qnt.breeze

import breeze.linalg.{DenseVector, SliceVector, Tensor, TensorLike, Vector}

import scala.reflect.ClassTag

class Series[I, V, IDX <: AbstractIndexVector[I], VEC <: Vector[V]]
(val index: IDX, val data: VEC) (implicit iTag:ClassTag[I], vTag: ClassTag[V])
  extends Tensor[I, V] with TensorLike[I, V, Series[I, V, IDX, VEC]]
    with Loc1dOps[I, V, Series[I, V, SliceIndexVector[I], SliceVector[Int, V]]]
    with Iloc1dOps[I, V, Series[I, V, SliceIndexVector[I], SliceVector[Int, V]]]
{

  if (index.length != data.length) {
    throw new IllegalArgumentException("index.length != values.length")
  }

  if (!index.unique) {
    throw new IllegalArgumentException("index must be unique")
  }

  override def apply(i: I): V = data(index.indexOfExactUnsafe(i))

  override def update(i: I, v: V): Unit = data(index.indexOfExactUnsafe(i)) = v

  override def size: Int = data.size

  override def activeSize: Int = index.size

  override val keySet: Set[I] = index.toSet

  override def keysIterator: Iterator[I] = keySet.iterator

  override def activeKeysIterator: Iterator[I] = keysIterator

  override def iterator: Iterator[(I, V)] = index.valuesIterator.zip(data.valuesIterator)

  override def activeIterator: Iterator[(I, V)] = iterator

  override def valuesIterator: Iterator[V] = data.valuesIterator

  override def activeValuesIterator: Iterator[V] = valuesIterator

  override def repr: Series[I, V, IDX, VEC] = this

  override def at(v: I): Option[V] = index.at(v).map(i => data(i))

  override def loc(vals: IterableOnce[I]): Series[I, V, SliceIndexVector[I], SliceVector[Int, V]] = {
    val idx = index.loc(vals)
    Series[I, V](idx, data(idx.slices))
  }

  override def loc(start: I, end: I, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
  : Series[I, V, SliceIndexVector[I], SliceVector[Int, V]] = {
    val idx = index.loc(start, end, step, keepEnd, keepEnd, round)
    Series[I, V](idx, data(idx.slices))
  }

  override def iat(i: Int): V = data(i)

  override def iloc(vals: IterableOnce[Int]): Series[I, V, SliceIndexVector[I], SliceVector[Int, V]] = {
    val idx = index.iloc(vals)
    Series[I, V](idx, data(idx.slices))
  }

  def copy: Series[I, V, IndexVector[I], DenseVector[V]]
    = Series[I, V](index.copy, data.copy.asInstanceOf[DenseVector[V]])

  override def toString: String = toString(5, 5)

  def toString(head: Int, tail:Int, indexFormat:I=>String = _.toString, dataFormat: V=>String = _.toString): String = {
    val rows = Math.min(index.length, head+tail+1)
    val output = Array.ofDim[String](rows+2, 2)
    output(0)(0) = s"Series[${iTag.runtimeClass.getSimpleName},${vTag.runtimeClass.getSimpleName}]:"
    output(0)(1) = ""
    output(1)(0) = s"index"
    output(1)(1) = s"value"
    for(i <- 0 until rows) {
      val j = if (i < head) i else (index.size - rows + i)
      output(2+i)(0) = indexFormat(index(j))
      output(2+i)(1) = dataFormat(data(j))
      if (rows < index.size && i == head) {
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
}

object Series {
  def apply[I,V]:Series.Builder[I,V] = Builder.asInstanceOf[Builder[I, V]]

  class Builder[I, V]  {
    def apply[IDX <: AbstractIndexVector[I], VEC <: Vector[V]](index: IDX, values: VEC)
                                                              (implicit iTag: ClassTag[I], vTag: ClassTag[V])
      = new Series[I,V,IDX,VEC](index, values)
  }

  object Builder extends Builder[Any, Any]

}

