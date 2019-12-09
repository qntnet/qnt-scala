package qnt.bz

trait Slice2dOps[R, C, S] {

  def iloc(rows: IndexedSeq[Int], cols: IndexedSeq[Int]): S

  def mask(rows: IndexedSeq[Boolean], cols: IndexedSeq[Boolean]): S

  def loc(rows: IndexedSeq[R], cols: IndexedSeq[C]): S

}
