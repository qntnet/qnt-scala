package qnt.bz

import breeze.linalg.{DenseMatrix, Matrix, SliceMatrix}
import breeze.math.Semiring
import qnt.bz.DataFrame.fill

import scala.reflect.ClassTag


class DataFrame[R, C, @specialized(Double, Int, Float, Long) V]
(
  val rowIdx: IndexVector[R],
  val colIdx: IndexVector[C],
  val data: Matrix[V]
)
(implicit val rTag: ClassTag[R], val cTag: ClassTag[C], val vTag: ClassTag[V], val vSem: Semiring[V])
  extends scala.collection.Map[(R, C),V]
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

  def apply(r: R, c: C): V = data(rowIdx.hashIndexOfUnsafe(r), colIdx.hashIndexOfUnsafe(c))

  def update(i: (R, C), v: V): Unit = update(i._1, i._2, v)

  def update(r: R, c: C, v: V): Unit = data((rowIdx.hashIndexOfUnsafe(r), colIdx.hashIndexOfUnsafe(c))) = v

  override def knownSize: Int = rowIdx.length * colIdx.length

  override def iterator: Iterator[((R, C), V)] = data.iterator.map(v => ((rowIdx(v._1._1), colIdx(v._1._2)), v._2))

  override def toString: String = toString(5, 5, 5, 5)

  def toString
  (
    headRows: Int = 5, tailRows: Int = 5,
    headCols: Int = 5, tailCols: Int = 5,
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

    override def locRange(start: R, end: R, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
    : DataFrame[R, C, V] = iloc(rowIdx.locRange(start, end, step, keepStart, keepEnd, round).slices)
  }

  object colOps extends Slice1dOps[C, DataFrame[R, C, V]] {

    override def size: Int = colIdx.size

    override def iloc(vals: IndexedSeq[Int]): DataFrame[R, C, V]
    = DataFrame.this.iloc(0 until rowIdx.size, vals)

    override def loc(vals: IndexedSeq[C]): DataFrame[R, C, V]
    = iloc(colIdx.loc(vals).slices)

    override def locRange(start: C, end: C, step: Int, keepStart: Boolean, keepEnd: Boolean, round: Boolean)
    : DataFrame[R, C, V] = iloc(colIdx.locRange(start, end, step, keepStart, keepEnd, round).slices)
  }

  def withIdx[R, C](rows: IndexVector[R], cols: IndexVector[C])
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

  def align(right: DataFrame[R,C,V], align: Align.AlignType, missingValue: V): DataFrame[R,C,V] = {
    val left = this
    val rowIdx = left.rowIdx.align(right.rowIdx, align)
    val colIdx = left.colIdx.align(right.colIdx, align)
    this.align(rowIdx, colIdx, missingValue)
  }

  def align(rowIdx:IndexVector[R], colIdx: IndexVector[C], missingValue: V): DataFrame[R, C, V] = {
    if(this.rowIdx == rowIdx && this.colIdx == colIdx) this
    else {
      val containsAllRows = rowIdx.forall(e => this.rowIdx.contains(e))
      val containsAllCols = colIdx.forall(e => this.colIdx.contains(e))
      if(containsAllCols && containsAllRows) {
        loc(rowIdx.toIndexedSeq, colIdx.toIndexedSeq).withIdx(rowIdx, colIdx)
      } else  {
        var df = fill(rowIdx, colIdx, missingValue)
        var ir = this.rowIdx.intersect(rowIdx)
        var ic = this.colIdx.intersect(colIdx)
        for (r <- ir.toIndexedSeq; c <- ic.toIndexedSeq) {
          df(r,c) = this(r,c)
        }
        df
      }
    }
  }

  def combine(frames: Seq[DataFrame[R, C, V]], missingValue: V)
  : DataFrame[R, C, V] = {
    val first = this

    val rowIdx = first.rowIdx.combine(frames.map(_.rowIdx))
    val colIdx = first.colIdx.combine(frames.map(_.colIdx))

    val result = fill(rowIdx, colIdx, missingValue)(this.rTag, this.cTag, this.vTag, this.vSem)

    for (f <- frames) {
      for (r <- f.rowIdx.toIndexedSeq; c <- f.colIdx.toIndexedSeq) {
        result(r, c) = f(r, c)
      }
    }
    result
  }

  def fillLike(value:V): DataFrame[R, C, V] = DataFrame.fill(rowIdx, colIdx, value)

  def ffillRows(missedValue: V): DataFrame[R,C,V] = {
    val result = copy
    for( ai <- colIdx.indices; ti <- 1 to rowIdx.indices.last) {
      if(result.data(ti, ai).equals(missedValue)) {
        result.data.update(ti,ai,result.data(ti-1,ai))
      }
    }
    result
  }

  def fillMissed(missedValue: V, fillValue: V): DataFrame[R,C,V] = {
    val result = copy
    for(ai <- colIdx.indices; ti <- 0 to rowIdx.indices.last) {
      if(result.data(ti, ai).equals(missedValue)) {
        result.data.update(ti, ai, fillValue)
      }
    }
    result
  }

  def shiftRows(period:Int, fillValue: V): DataFrame[R,C,V]  = {
    val result = copy
    if(period > 0) {
      for(ai <- colIdx.indices; ti <- rowIdx.indices) {
        if(ti < period) {
          result.data.update(ti, ai, fillValue)
        } else {
          result.data.update(ti,ai, this.data(ti - period, ai))
        }
      }
    } else if (period < 0) {
      for(ai <- colIdx.indices; ti <- rowIdx.indices) {
        if(ti > rowIdx.indices.last - period) {
          result.data.update(ti, ai, fillValue)
        } else {
          result.data.update(ti,ai, this.data(ti + period, ai))
        }
      }
    }
    result
  }

  def firstNotEmptyRow(missingValue: V): R = {
    for(r <- rowIdx; c <- colIdx) {
      if(!this(r,c).equals(missingValue)) {
        return r
      }
    }
    rowIdx.last
  }

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

  def transpose : DataFrame[C, R, V] = { // this is just logical transpose
    val dt:Matrix[V] = data match {
      case d: DenseMatrix[V] => d.t
      case _ => data.asInstanceOf[SliceMatrix[Int, Int, V]].toDenseMatrix.t
    }
    DataFrame(colIdx, rowIdx, dt)
  }

  override def -(key: (R, C)): collection.Map[(R, C), V] = ???

  override def -(key1: (R, C), key2: (R, C), keys: (R, C)*): collection.Map[(R, C), V] = ???

  override def get(key: (R, C)): Option[V] = rowIdx.hashIndexOf(key._1).zip(colIdx.hashIndexOf(key._2)).map(data.apply)
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
    DenseMatrix.fill(ridx.size, cidx.size)(fillValue)
  )

  def fromSeries[R:ClassTag,C:ClassTag:Ordering,V:ClassTag:Semiring](columns: (C, Series[R,V])*) : DataFrame[R, C, V] = {
    var columnIdx = DataIndexVector(columns.map(i=>i._1))
    val firstSeries: Series[R,V] = columns(0)._2
    var df : DataFrame[R, C, V] = fill(firstSeries.idx, columnIdx, firstSeries.data(0))
    for(c <- columns) {
      val colLabel = c._1
      val colSeries = c._2
      for(r <- colSeries.idx) {
        df(r,colLabel) = colSeries(r)
      }
    }
    df
  }

}