package qnt.breeze

import breeze.linalg.{Matrix, Tensor, TensorLike}

import scala.reflect.ClassTag


class DataFrame[R, C, V, RIDX <:AbstractIndexVector[R], CIDX <: AbstractIndexVector[C], DATA <: Matrix[V]]
(
  val rows: RIDX,
  val cols: CIDX,
  val data: DATA
) (implicit rTag:ClassTag[R], cTag: ClassTag[C], vTag:ClassTag[V])
  extends Tensor[(R, C), V] with TensorLike[(R, C), V, DataFrame[R,C,V, RIDX, CIDX, DATA]] {
  if (!rows.unique) {
    throw new IllegalArgumentException("rows index must be unique")
  }
  if (!cols.unique) {
    throw new IllegalArgumentException("cols index must be unique")
  }
  if(rows.size != data.rows) {
    throw new IllegalArgumentException("rows size mismatch")
  }
  if(cols.size != data.cols) {
    throw new IllegalArgumentException("cols size mismatch")
  }

  override def apply(i: (R, C)): V = data(rows.indexOfExactUnsafe(i._1), cols.indexOfExactUnsafe(i._2))

  override def update(i: (R, C), v: V): Unit
   = data((rows.indexOfExactUnsafe(i._1), cols.indexOfExactUnsafe(i._2))) = v

  override def size: Int = data.size

  override def activeSize: Int = data.size

  override def iterator: Iterator[((R, C), V)] = data.iterator.map(v => ((rows(v._1._1), cols(v._1._2)), v._2))

  override def activeIterator: Iterator[((R, C), V)] = iterator

  object keySet extends Set[(R, C)] {
    override def incl(elem: (R, C)): Set[(R, C)] = Set() ++ iterator + elem
    override def excl(elem: (R, C)): Set[(R, C)] = Set() ++ iterator + elem
    override def contains(elem: (R, C)): Boolean = rows.contains(elem._1) && cols.contains(elem._2)
    override def iterator: Iterator[(R, C)] = rows.valuesIterator.flatMap(r => cols.valuesIterator.map(c => (r, c)))
  }

  override def keysIterator: Iterator[(R, C)] = keySet.iterator

  override def activeKeysIterator: Iterator[(R, C)] = keySet.iterator

  override def valuesIterator: Iterator[V] = data.valuesIterator

  override def activeValuesIterator: Iterator[V] = data.valuesIterator

  override def repr: DataFrame[R, C, V, RIDX, CIDX, DATA] = this

  def apply(r:R, c:C) : V = data(rows.indexOfExactUnsafe(r), cols.indexOfExactUnsafe(c))

  override def toString: String = toString(5, 5, 5, 5)

  def toString
  (
    headRows: Int, tailRows:Int,
    headCols: Int, tailCols:Int,
    rowFormat:R=>String = _.toString,
    colFormat:C=>String = _.toString,
    dataFormat: V=>String = _.toString,
    minWidth: Int = 7
  ): String = {
    val rowCnt = Math.min(rows.size, headRows + tailRows + 1)
    val colCnt = Math.min(cols.size, headCols + tailCols + 1)

    val output = Array.ofDim[String](rowCnt+2, colCnt + 1)
    for(i <- output.indices; j <- output(0).indices) {
      output(i)(j) = ""
    }

    output(0)(0) = s"DataFrame[${rTag.runtimeClass.getSimpleName},${cTag.runtimeClass.getSimpleName},${vTag.runtimeClass.getSimpleName}]:"
    output(1)(0) = s"row\\col"

    for (i <- 0 until colCnt) {
      val j = if (i < headCols) i else (cols.size - colCnt + i)
      output(1)(1 + i) = colFormat(cols(j))
      if(colCnt < cols.size && i == headCols){
        output(1)(1 + i) = "..."
      }
    }

    for (i <- 0 until rowCnt) {
      val j = if (i < headRows) i else (rows.size - rowCnt + i)
      output(2+i)(0) = rowFormat(rows(j))
      if(rowCnt < rows.size && i == headRows){
        output(2+i)(0) = "..."
      }
    }

    for (i <- 0 until rowCnt; j <- 0 until colCnt) {
      val mi = if (i < headRows) i else (rows.size - rowCnt + i)
      val mj = if (j < headCols) j else (cols.size - colCnt + j)
      output(2+i)(1+j) = dataFormat(data(mi, mj))
      if(colCnt < cols.size && j == headCols || rowCnt < rows.size && i == headRows){
        output(2+i)(1 + j) = "..."
      }
    }

    // adjust column size
    for(c <- output(0).indices) {
      var size = minWidth
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

object DataFrame {

  def apply[R,C,V]: Builder[R, C, V] = Builder.asInstanceOf[Builder[R,C,V]]

  class Builder[R, C, V] {
    def apply[RIDX<:AbstractIndexVector[R], CIDX <: AbstractIndexVector[C], DATA <: Matrix[V]]
      (ridx:RIDX, cidx:CIDX, data:DATA) (implicit rTag:ClassTag[R], cTag: ClassTag[C], vTag:ClassTag[V])
      :DataFrame[R, C, V, RIDX, CIDX, DATA] = new DataFrame[R, C, V, RIDX, CIDX, DATA](ridx, cidx, data)
  }

  object Builder extends Builder[Any, Any, Any]{}
}