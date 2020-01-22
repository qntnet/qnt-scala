package ai.quantnet

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import ai.quantnet.bz.{DataFrame, DataIndexVector, Series}
import breeze.linalg.DenseMatrix
import org.apache.commons.io.IOUtils
import ucar.ma2.DataType
import ucar.nc2.{Attribute, NetcdfFile, NetcdfFileWriter, Variable}

object netcdf {
  def toCharSeqFxdSize(str: String, size: Int):Array[Char] = {
    var res = new Array[Char](size)
    var charStr = str.toCharArray
    Array.copy(charStr, 0, res, 0, charStr.length)
    res
  }

  def dataFrameToNetcdf(df: DataFrame[LocalDate, String, Double]): Array[Byte] = {
    val tmpFile = File.createTempFile("df-", ".nc")

    try {
      val f = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, tmpFile.getAbsolutePath)
      try {
        f.addDimension(null, "time", df.rowIdx.length)
        f.addDimension(null, "asset", df.colIdx.length)
        val maxAssetLen = df.colIdx.map(s => s.length).max
        val assetStrDim = "str" + maxAssetLen
        f.addDimension(null, assetStrDim, maxAssetLen)

        val timeVar = f.addVariable(null, "time", DataType.INT, "time")
        val zeroDate = df.rowIdx(0)
        timeVar.addAttribute(new Attribute("units", s"days since $zeroDate"))
        timeVar.addAttribute(new Attribute("calendar", "gregorian"))

        val assetVar = f.addVariable(null, "asset", DataType.CHAR, "asset " + assetStrDim)
        assetVar.addAttribute(new Attribute("_Encoding", "utf-8"))

        val dataVar = f.addVariable(null, "__xarray_dataarray_variable__", DataType.DOUBLE, "time asset")
        dataVar.addAttribute(new Attribute("_FillValue", Double.NaN))

        f.create()

        val timeArr = df.rowIdx.toArray.map(t => ChronoUnit.DAYS.between(zeroDate, t).intValue)
        f.write(timeVar, ucar.ma2.Array.factory(timeArr))
        f.write(assetVar, ucar.ma2.Array.factory(df.colIdx.toArray.map(s => netcdf.toCharSeqFxdSize(s, maxAssetLen))))
        f.write(dataVar, ucar.ma2.Array.factory(
          DataType.DOUBLE,
          Array[Int](df.rowIdx.length, df.colIdx.length),
          df.data.toDenseMatrix.t.toArray
        ))

      } finally {
        f.close()
      }
      val fis = new FileInputStream(tmpFile)
      try {
        val result = new ByteArrayOutputStream()
        IOUtils.copy(fis, result)
        result.toByteArray
      } finally {
        fis.close()
      }
    } finally {
      tmpFile.delete()
    }
  }

  def seriesToNetcdf(series: Series[LocalDate, Double]): Array[Byte] = {
    val tmpFile = File.createTempFile("series-", ".nc")

    try {
      val f = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, tmpFile.getAbsolutePath)
      try {
        f.addDimension(null, "time", series.idx.length)
        val timeVar = f.addVariable(null, "time", DataType.INT, "time")
        val zeroDate = series.idx(0)
        timeVar.addAttribute(new Attribute("units", s"days since $zeroDate"))
        timeVar.addAttribute(new Attribute("calendar", "gregorian"))

        val dataVar = f.addVariable(null, "__xarray_dataarray_variable__", DataType.DOUBLE, "time")
        dataVar.addAttribute(new Attribute("_FillValue", Double.NaN))

        f.create()

        val timeArr = series.idx.toArray.map(t => ChronoUnit.DAYS.between(zeroDate, t).intValue)
        f.write(timeVar, ucar.ma2.Array.factory(timeArr))
        f.write(dataVar, ucar.ma2.Array.factory(series.data.toArray))

      } finally {
        f.close()
      }
      val fis = new FileInputStream(tmpFile)
      try {
        val result = new ByteArrayOutputStream()
        IOUtils.copy(fis, result)
        result.toByteArray
      } finally {
        fis.close()
      }
    } finally {
      tmpFile.delete()
    }
  }

  def netcdf2DToFrames(bytes: Array[Byte]): DataFrame[LocalDate, String, Double] = {
    val dataNetcdf = NetcdfFile.openInMemory("data", bytes)

    val vars = dataNetcdf.getVariables
      .toArray(new Array[Variable](dataNetcdf.getVariables.size()))
      .map(v=>(v.getShortName, v)).toMap

    val timeVar = vars("time")
    val zeroDateStr = timeVar.getAttributes
      .toArray(new Array[Attribute](timeVar.getAttributes.size()))(0)
      .getStringValue
    val zeroDate = LocalDate.parse(zeroDateStr.split(" since ")(1).split(" ")(0))
    val timeRawArray = dataNetcdf.readSection("time").copyTo1DJavaArray().asInstanceOf[Array[Int]]
    val timeArray = timeRawArray.map(i => zeroDate.plusDays(i))

    val assetVar = vars("asset")
    val assetRawArray = dataNetcdf.readSection("asset").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val assetArray = assetRawArray.map(i => new String(i.filter(c => c != '\u0000')))

    // C-order of dimensions: time,asset
    val varName = vars.keys.filter(vars(_).getDataType == DataType.DOUBLE).toArray.apply(0)
    val values = dataNetcdf.readSection(varName).copyTo1DJavaArray().asInstanceOf[Array[Double]]

    val timeIdx = DataIndexVector[LocalDate](timeArray)
    val assetIdx = DataIndexVector[String](assetArray)
    val valueMatrix = DenseMatrix.create(timeArray.length, assetArray.length, values, 0, assetArray.length, true)

    DataFrame[LocalDate, String, Double](timeIdx, assetIdx, valueMatrix)
  }

  def netcdf3DToFrames(bytes: Array[Byte])
  : Map[String,  DataFrame[LocalDate, String, Double]]
    = intern {
    val dataNetcdf = NetcdfFile.openInMemory("data", bytes)

    val vars = dataNetcdf.getVariables
      .toArray(new Array[Variable](dataNetcdf.getVariables.size()))
      .map(v=>(v.getShortName, v))
      .toMap

    val timeVar = vars("time")
    val zeroDateStr = timeVar.getAttributes
      .toArray(new Array[Attribute](timeVar.getAttributes.size()))
      .filter(i=>i.getShortName == "units")(0)
      .getStringValue

    val zeroDate = LocalDate.parse(zeroDateStr.split(" since ")(1).split(" ")(0))
    val timeRawArray = dataNetcdf.readSection("time").copyTo1DJavaArray().asInstanceOf[Array[Int]]
    val timeArray = timeRawArray.map(i => intern(zeroDate.plusDays(i)))

    var fieldVar = vars("field")
    val fieldRawArray = dataNetcdf.readSection("field").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val fieldArray = fieldRawArray.map(i => intern(new String(i.filter(c => c != '\u0000'))))

    val assetVar = vars("asset")
    val assetRawArray = dataNetcdf.readSection("asset").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val assetArray = assetRawArray.map(i => intern(new String(i.filter(c => c != '\u0000'))))

    // C-order of dimensions: field,time,asset
    val varName = vars.keys.filter(vars(_).getDataType == DataType.DOUBLE).filter(_!="time").toArray.apply(0)
    val values = dataNetcdf.readSection(varName).copyTo1DJavaArray().asInstanceOf[Array[Double]]

    val timeIdx = DataIndexVector.apply[LocalDate](timeArray)
    val assetIdx = DataIndexVector[String](assetArray)
    val valueMatrices = fieldArray.indices
      .map(i => DenseMatrix.create(timeArray.length, assetArray.length, values, i * timeArray.length * assetArray.length, assetArray.length, true))
      .toArray

    var result = Map[String, DataFrame[LocalDate, String, Double]]()

    for(fi <- fieldArray.indices) {
      val field = fieldArray(fi)
      val mat = valueMatrices(fi)
      val frame = DataFrame[LocalDate, String, Double](timeIdx, assetIdx, mat)
      result += (field -> frame)
    }
    result
  }
}
