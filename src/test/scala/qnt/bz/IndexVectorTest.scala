package qnt.bz
import breeze.linalg.{DenseMatrix, DenseVector}
import org.scalatest.FunSuite

class IndexVectorTest extends FunSuite {

  test("loadStockDailySeries") {
    val idx = DataIndexVector[String](Array("1", "2", "3", "5", "8").reverse, true, true, true)
    var s = idx.iloc(Seq(1,4,3))
    //s(1) = "2"

    println(idx.mask(idx.iterator.map(_ > "2").toIndexedSeq))

    var vals = DenseVector.apply(1,3,4,6,8)

    var ser = Series[String,Int](idx, vals)

    val c = ser.copy
    //c.iloc(0, 3) := 3

    println(c.toString(1,2))

    var r = c.iloc(0,2,3,4)

    var mat = DenseMatrix.create(idx.size, r.size, (1 to (idx.size*r.size)).toArray)

    var frame = DataFrame(idx, r.idx, mat)

    println(frame.toString(1,1,1,1))

  }


}
