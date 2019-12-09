package qnt.bz

import breeze.linalg.operators.{OpDiv, OpMulMatrix}
import breeze.linalg.support.CanSlice2
import breeze.linalg.{DenseMatrix, Matrix, SliceMatrix, Tensor, TensorLike}
import breeze.math.Semiring

import scala.reflect.ClassTag


class DataFrame[R, C, @specialized(Double, Int, Float, Long) V]
(
  val rowIdx: IndexVector[R],
  val colIdx: IndexVector[C],
  val data: Matrix[V]
)
(implicit val rTag: ClassTag[R], val cTag: ClassTag[C], val vTag: ClassTag[V], val vSem: Semiring[V])
  extends Tensor[(R, C), V]
    with TensorLike[(R, C), V, DataFrame[R, C, V]]
    with Slice2dOps[R,C,DataFrame[R,C,V]] {

  if (!rowIdx.unique) {
    throw new IllegalArgumentException("rows index must be unique")
  }
  if (!colIdx.unique) {
    throw new IllegalArgumentException("cols index must be unique")
  }
  if (rowIdx.size != data.rows) {
    throw new IllegalArgumentException("rows size mismatch")
  }
  if (colIdx.size != data.cols) {
    throw new IllegalArgumentException("cols size mismatch")
  }

  override def apply(i: (R, C)): V = apply(i._1, i._2)

  def apply(r: R, c: C): V = data(rowIdx.indexOfExactUnsafe(r), colIdx.indexOfExactUnsafe(c))

  override def update(i: (R, C), v: V): Unit
  = update(i._1, i._2, v)

  def update(r: R, c: C, v: V): Unit
  = data((rowIdx.indexOfExactUnsafe(r), colIdx.indexOfExactUnsafe(c))) = v

  override def size: Int = data.size

  override def activeSize: Int = data.size

  override def iterator: Iterator[((R, C), V)] = data.iterator.map(v => ((rowIdx(v._1._1), colIdx(v._1._2)), v._2))

  override def activeIterator: Iterator[((R, C), V)] = iterator

  object keySet extends Set[(R, C)] {
    override def incl(elem: (R, C)): Set[(R, C)] = Set() ++ iterator + elem

    override def excl(elem: (R, C)): Set[(R, C)] = Set() ++ iterator + elem

    override def contains(elem: (R, C)): Boolean = rowIdx.contains(elem._1) && colIdx.contains(elem._2)

    override def iterator: Iterator[(R, C)] = rowIdx.valuesIterator.flatMap(r => colIdx.valuesIterator.map(c => (r, c)))
  }

  override def keysIterator: Iterator[(R, C)] = keySet.iterator

  override def activeKeysIterator: Iterator[(R, C)] = keySet.iterator

  override def valuesIterator: Iterator[V] = data.valuesIterator

  override def activeValuesIterator: Iterator[V] = data.valuesIterator

  override def repr: DataFrame[R, C, V] = this

  override def toString: String = toString(5, 5, 5, 5)

  def toString
  (
    headRows: Int, tailRows: Int,
    headCols: Int, tailCols: Int,
    rowFormat: R => String = _.toString,
    colFormat: C => String = _.toString,
    dataFormat: V => String = _.toString,
    minWidth: Int = 7
  ): String = {
    val rowCnt = Math.min(rowIdx.size, headRows + tailRows + 1)
    val colCnt = Math.min(colIdx.size, headCols + tailCols + 1)

    val output = Array.ofDim[String](rowCnt + 2, colCnt + 1)
    for (i <- output.indices; j <- output(0).indices) {
      output(i)(j) = ""
    }

    output(0)(0) = s"DataFrame[${rTag.runtimeClass.getSimpleName},${cTag.runtimeClass.getSimpleName},${vTag.runtimeClass.getSimpleName}]:"
    output(1)(0) = s"row\\col"

    for (i <- 0 until colCnt) {
      val j = if (i < headCols) i else (colIdx.size - colCnt + i)
      output(1)(1 + i) = colFormat(colIdx(j))
      if (colCnt < colIdx.size && i == headCols) {
        output(1)(1 + i) = "..."
      }
    }

    for (i <- 0 until rowCnt) {
      val j = if (i < headRows) i else (rowIdx.size - rowCnt + i)
      output(2 + i)(0) = rowFormat(rowIdx(j))
      if (rowCnt < rowIdx.size && i == headRows) {
        output(2 + i)(0) = "..."
      }
    }

    for (i <- 0 until rowCnt; j <- 0 until colCnt) {
      val mi = if (i < headRows) i else (rowIdx.size - rowCnt + i)
      val mj = if (j < headCols) j else (colIdx.size - colCnt + j)
      output(2 + i)(1 + j) = dataFormat(data(mi, mj))
      if (colCnt < colIdx.size && j == headCols || rowCnt < rowIdx.size && i == headRows) {
        output(2 + i)(1 + j) = "..."
      }
    }

    // adjust column size
    for (c <- output(0).indices) {
      var size = minWidth
      for (r <- 1 until output.length) {
        size = Math.max(size, output(r)(c).length)
      }
      for (r <- 1 until output.length) {
        output(r)(c) = output(r)(c) + " " * (size - output(r)(c).length)
      }
    }
    output.map(r => r.mkString(" ")).mkString("\n")
  }

  def copy: DataFrame[R, C, V] = new DataFrame(rowIdx.copy, colIdx.copy, data.copy)

  override def iloc(rows: IndexedSeq[Int], cols: IndexedSeq[Int]): DataFrame[R, C, V] = {
    DataFrame(rowIdx.iloc(rows), colIdx.iloc(cols), new SliceMatrix(data, rows, cols))
  }

  override def mask(rows: IndexedSeq[Boolean], cols: IndexedSeq[Boolean]): DataFrame[R, C, V] = {
    val rowIdx = rows.iterator.zipWithIndex.filter(_._1).map(_._2).toArray
    val colIdx = cols.iterator.zipWithIndex.filter(_._1).map(_._2).toArray
    iloc(rowIdx, colIdx)
  }

  override def loc(rows: IndexedSeq[R], cols: IndexedSeq[C]): DataFrame[R, C, V]
  = iloc(rowIdx.loc(rows).slices, colIdx.loc(cols).slices)


  object rowOps extends Slice1dOps[R, DataFrame[R, C, V]] {

    override def size: Int = rowIdx.size

    override def iloc(vals: IndexedSeq[Int]): DataFrame[R, C, V]
    = DataFrame.this.iloc(vals, 0 until colIdx.size)

    override def loc(vals: IndexedSeq[R]): DataFrame[R, C, V]
    = iloc(rowIdx.loc(vals).slices)

    override def loc(start: R, end: R, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
    : DataFrame[R, C, V] = iloc(rowIdx.loc(start, end, step, keepStart, keepEnd, round).slices)
  }

  object colOps extends Slice1dOps[C, DataFrame[R, C, V]] {

    override def size: Int = colIdx.size

    override def iloc(vals: IndexedSeq[Int]): DataFrame[R, C, V]
    = DataFrame.this.iloc(0 until rowIdx.size, vals)

    override def loc(vals: IndexedSeq[C]): DataFrame[R, C, V]
    = iloc(colIdx.loc(vals).slices)

    override def loc(start: C, end: C, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
    : DataFrame[R, C, V] = iloc(colIdx.loc(start, end, step, keepStart, keepEnd, round).slices)
  }

  def reIndex[R, C](rows: IndexVector[R], cols: IndexVector[C])
  : DataFrame[R, C, V] = DataFrame(rows, cols, data)(rTag = rows.tag, cTag = cols.tag, vTag = vTag, vSem = vSem)

  def intersect(another: DataFrame[R, C, V]): DataFrame[R, C, V] = {
    if (rowIdx == another.rowIdx && colIdx == another.colIdx) {
      this
    } else {
      val rows = rowIdx.intersect(another.rowIdx)
      val cols = colIdx.intersect(another.colIdx)
      iloc(rows.slices, cols.slices)
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[DataFrame[R, C, V]]

  override def equals(other: Any): Boolean = other match {
    case that: DataFrame[R, C, V] =>
      (that canEqual this) &&
        rowIdx == that.rowIdx &&
        colIdx == that.colIdx &&
        data == that.data
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), rowIdx, colIdx, data)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object DataFrame {

  def apply[R, C, @specialized(Double, Int, Float, Long) V](ridx: IndexVector[R], cidx: IndexVector[C], data: Matrix[V])
                                                           (implicit rTag: ClassTag[R], cTag: ClassTag[C], vTag: ClassTag[V], vSem: Semiring[V])
  : DataFrame[R, C, V] = new DataFrame[R, C, V](ridx, cidx, data)

  def fill[R, C, @specialized(Double, Int, Float, Long) V](ridx: IndexVector[R], cidx: IndexVector[C], fillValue: V)
                                                          (implicit rTag: ClassTag[R], cTag: ClassTag[C], vTag: ClassTag[V], vSem: Semiring[V])
  : DataFrame[R, C, V] = new DataFrame[R, C, V](
    ridx,
    cidx,
    DenseMatrix.create(ridx.size, cidx.size, new Array[V](ridx.size * cidx.size))
  )

  def combine[R, C, @specialized(Double, Int, Float, Long) V](frames: Seq[DataFrame[R, C, V]], missingValue: V): DataFrame[R, C, V] = {
    val first = frames(0)
    import first._

    val rowIdx = IndexVector.combine(frames.map(_.rowIdx))
    val colIdx = IndexVector.combine(frames.map(_.colIdx))

    val result = fill(rowIdx, colIdx, missingValue)

    for (f <- frames) {
      for (r <- f.rowIdx.toIndexedSeq; c <- f.colIdx.toIndexedSeq) {
        result(r, c) = f(r, c)
      }
    }
    result
  }

  implicit def canSlice2[R, C, V]: CanSlice2[DataFrame[R, C, V], R, C, V]
  = new CanSlice2[DataFrame[R, C, V], R, C, V] {
    override def apply(from: DataFrame[R, C, V], slice: R, slice2: C): V = from.apply(slice, slice2)
  }


  implicit def divOps2[R, C, @specialized(Double, Float, Int, Long) V]
  : OpDiv.Impl2[DataFrame[R, C, V], DataFrame[R, C, V], DataFrame[R, C, V]] =
    new OpDiv.Impl2[DataFrame[R, C, V], DataFrame[R, C, V], DataFrame[R, C, V]]() {
      override def apply(v: DataFrame[R, C, V], v2: DataFrame[R, C, V]): DataFrame[R, C, V] = {
        val result = v.intersect(v2).copy
        for (c <- result.colIdx.toIndexedSeq; r <- result.rowIdx.toIndexedSeq) {
          result(r, c) = (v(r, c).asInstanceOf[Double] / v(r, c).asInstanceOf[Double]).asInstanceOf[V]
        }
        result
      }
    }


  implicit def mulOps2[R, C, @specialized(Double, Int, Float, Long) V]: OpMulMatrix.Impl2[DataFrame[R, C, V], DataFrame[R, C, V], DataFrame[R, C, V]] =
    new OpMulMatrix.Impl2[DataFrame[R, C, V], DataFrame[R, C, V], DataFrame[R, C, V]] {
      override def apply(v: DataFrame[R, C, V], v2: DataFrame[R, C, V]): DataFrame[R, C, V] = {
        import v._
        if (v.rowIdx == v2.rowIdx && v.colIdx == v2.colIdx) {
          DataFrame(v.rowIdx, v2.colIdx, v.data.toDenseMatrix *:* v2.data.toDenseMatrix)
        } else {
          val result = v.intersect(v2).copy
          for (c <- result.colIdx.toIndexedSeq; r <- result.rowIdx.toIndexedSeq) {
            result(r, c) = (v(r, c).asInstanceOf[Double] * v2(r, c).asInstanceOf[Double]).asInstanceOf[V]
          }
          result
        }
      }
    }
}