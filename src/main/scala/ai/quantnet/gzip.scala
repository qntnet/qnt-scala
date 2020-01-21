package ai.quantnet

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

object gzip {
  def gzipCompress(uncompressedData: Array[Byte]): Array[Byte] = {
    var result = Array[Byte]()
    val bos = new ByteArrayOutputStream(uncompressedData.length)
    val gzipOS = new GZIPOutputStream(bos)
    try {
      gzipOS.write(uncompressedData)
      // You need to close it before using bos
      gzipOS.close()
      result = bos.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      if (bos != null) bos.close()
      if (gzipOS != null) gzipOS.close()
    }
    result
  }

  def gzipUncompress(compressedData: Array[Byte]): Array[Byte] = {
    var result = Array[Byte]()
    val bis = new ByteArrayInputStream(compressedData)
    val bos = new ByteArrayOutputStream
    val gzipIS = new GZIPInputStream(bis)
    try {
      val buffer = new Array[Byte](1024)
      var b = 0
      do {
        b = gzipIS.read(buffer)
        if(b >= 0) {
          bos.write(buffer, 0, b)
        }
      } while (b >= 0)
      result = bos.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      if (bis != null) bis.close()
      if (bos != null) bos.close()
      if (gzipIS != null) gzipIS.close()
    }
    result
  }
}
