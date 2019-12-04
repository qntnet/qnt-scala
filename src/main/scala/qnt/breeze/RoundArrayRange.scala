package qnt.breeze

class RoundArrayRange(limit:Int, start:Int, step:Int, count:Int) extends IndexedSeq[Int]{

  override def apply(i: Int): Int = {
    if(i < 0 || i >= count) {
      new IllegalArgumentException("out of range")
    }
    (start + i * step) % limit
  }

  override def length: Int = count
}

object RoundArrayRange {

  def apply(
            length:Int,

            start:Int = 0,
            end:Int = -1,
            step:Int = 1,

            left:Boolean = true,
            right: Boolean = true,
            round: Boolean = true
          ):RoundArrayRange = {

    val realStart = (if(start > 0) start else (length + start)) + (if(left) 0 else 1)

    if(realStart < 0 || realStart >= length) {
       throw new IllegalArgumentException("out of range start")
    }

    val realEnd = (if(end > 0) end else (length + end)) + (if(right) 0 else -1)
    if(realEnd < 0 || realEnd >= length) {
      if(realStart < 0 || realStart >= length) {
        throw new IllegalArgumentException("out of range end")
      }
    }

    val dist = realEnd - realStart

    val ustep = step * (if (step < 0) -1 else 1)
    var udist = step * (if (step < 0) -1 else 1)

    if(round && udist < 0) {
      udist += length
    }

    val cnt = if (udist < 0 || length < 1) 0  else udist / ustep

    new RoundArrayRange(length, realStart, step, cnt)
  }

}