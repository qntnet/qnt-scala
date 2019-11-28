package qnt

import java.net.{HttpURLConnection, URL}
import java.time.LocalDate

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.saddle.{Frame, Index, Mat}
import org.slf4j.LoggerFactory
import ucar.nc2.NetcdfFile

import scala.collection.JavaConversions._
import scala.collection.immutable.{Map => ImmutableMap}
import scala.util.control.NonFatal



object data {
  case class StockInfo (
                         id: String,
                         cik: Option[String],
                         exchange: String,
                         symbol: String,
                         name: String,
                         sector: Option[String],
                         props: ImmutableMap[String, Any]
                       ) {

    def this(props: Map[String, Any]) = this(
      props("id").asInstanceOf[String],
      //props("FIGI").asInstanceOf[String],
      props.get("cik") match {
        case Some(s) => {
          val str = s.asInstanceOf[String]
          if (str != null && str.length > 0) Some(str) else None
        }
        case _ => None
      },
      props("exchange").asInstanceOf[String],
      props("symbol").asInstanceOf[String],
      props("name").asInstanceOf[String],
      {
        val s = props("sector").asInstanceOf[String]
        if (s == null || s.length < 1) None else Some(s)
      },
      props - "id" - "cik" - "exchange" - "symbol" - "name" - "sector"
    )
  }

  object Fields {
    val open = "open"
    val low = "low"
    val high = "high"
    val close = "close"
    val vol = "vol"
    val divs = "divs"
    val split = "split"
    val split_cumprod = "split_cumprod"
    val is_liquid = "is_liquid"

    val values = List(open, low, high, close, vol, divs, split, split_cumprod, is_liquid )
  }

  def loadStockList(
                     minDate: LocalDate = LocalDate.of(2007, 1, 1),
                     maxDate: LocalDate = LocalDate.now()
                   ) : List[StockInfo] = {

    val uri = s"assets?min_date=$minDate&max_date=$maxDate"
    val dataBytes = loadWithRetry(uri)
    if(dataBytes == null) {
      return List.empty
    }
    val lst = OBJECT_MAPPER.readValue[List[Map[String, Any]]](dataBytes)
    lst.map(p => new StockInfo(p))
  }

  def loadStockDailySeries(
                            ids: List[String],
                            minDate: LocalDate = LocalDate.of(2007, 1, 1),
                            maxDate: LocalDate = LocalDate.now()
                          ) : Map[String, Frame[LocalDate, String, Double]] = {
    var uri = "data"
    var params = Map(
      "assets" -> ids,
      "min_date" -> minDate.toString,
      "max_date" -> maxDate.toString
    )
    val dataBytes = loadWithRetry(uri, params)
    netcdfBytesToFrame(dataBytes)
  }

  def loadIndexList() = ???

  def loadIndexSeries() = ???


  private val LOG = LoggerFactory.getLogger(getClass)
  private val RETRIES = 5
  private val TIMEOUT = 60*1000
  private val OBJECT_MAPPER = new ObjectMapper() with ScalaObjectMapper

  OBJECT_MAPPER.registerModule(DefaultScalaModule)

  private def loadStockDailySeriesOriginChunk(
                                               ids: List[String],
                                               minDate: LocalDate = LocalDate.of(2007, 1, 1),
                                               maxDate: LocalDate = LocalDate.now()
                                             ) : Map[String, Frame[LocalDate, String, Double]] = {
    var uri = "data"
    var params = Map(
      "assets" -> ids,
      "min_date" -> minDate.toString,
      "max_date" -> maxDate.toString
    )
    val dataBytes = loadWithRetry(uri, params)
    netcdfBytesToFrame(dataBytes)
  }

  private def netcdfBytesToFrame(bytes: Array[Byte]): Map[String, Frame[LocalDate, String, Double]] = {
    val dataNetcdf = NetcdfFile.openInMemory("data", bytes)

    val vars = dataNetcdf.getVariables.toList.map(v=>(v.getShortName, v)).toMap

    val timeVar = vars("time")
    val zeroDateStr = timeVar.getAttributes.toList.filter(i=>i.getShortName == "units").get(0).getStringValue
    val zeroDate = LocalDate.parse(zeroDateStr.split(" since ")(1))
    val timeRawArray = dataNetcdf.readSection("time").copyTo1DJavaArray().asInstanceOf[Array[Int]]
    val timeArray = timeRawArray.map(i => zeroDate.plusDays(i))

    var fieldVar = vars("field")
    val fieldRawArray = dataNetcdf.readSection("field").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val fieldArray = fieldRawArray.map(i => new String(i.filter(c => c != '\0')))

    val assetVar = vars("asset")
    val assetRawArray = dataNetcdf.readSection("asset").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val assetArray = assetRawArray.map(i => new String(i.filter(c => c != '\0')))

    // C-order of dimensions: field,time,asset
    val values = dataNetcdf.readSection("__xarray_dataarray_variable__").copyTo1DJavaArray().asInstanceOf[Array[Double]]

    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
    val timeIdx = Index[LocalDate](timeArray)
    val assetIdx = Index[String](assetArray)
    val valueMatrices = values.grouped(timeArray.length*assetArray.length).map(g => Mat(timeArray.length, assetArray.length, g)).toArray

    var result = Map[String, Frame[LocalDate, String, Double]]()

    for(fi <- fieldArray.indices) {
      val field = fieldArray(fi)
      val mat = valueMatrices(fi)
      val frame = Frame[LocalDate, String, Double](mat, timeIdx, assetIdx)
      val frameForward = frame.row(timeIdx.reversed.toVec.contents.sorted)

      result += (field -> frameForward)
    }

    print(Fields.values)
    result
  }

  private def loadWithRetry(uri: String, dataObj: Any = null): Array[Byte] = {
    val urlStr = baseUrl + uri
    val url = new URL(urlStr)
    for (t <- 1 to RETRIES) {
      var exc: Exception = null
      var conn: HttpURLConnection = null
      try {
        conn = url.openConnection().asInstanceOf[HttpURLConnection]
        conn.setConnectTimeout(TIMEOUT)
        conn.setReadTimeout(TIMEOUT)
        conn.setDoOutput(dataObj != null)
        conn.setRequestMethod(if(dataObj == null) "GET" else "POST")
        conn.setUseCaches(false)
        conn.setDoInput(true)
        if(dataObj != null) {
          val dataBytes = OBJECT_MAPPER.writeValueAsBytes(dataObj)
          // conn.setRequestProperty("Content-Type", "application/json; utf-8")
          conn.setRequestProperty("Content-Length", Integer.toString(dataBytes.length))
          conn.getOutputStream.write(dataBytes)
          conn.getOutputStream.flush()
        }
        val code = conn.getResponseCode
        if(code == 404) {
          return null
        }
        if(code != 200) {
          throw new IllegalStateException(s"Wrong status code: $code")
        }
        val is = conn.getInputStream
        return is.readAllBytes()
      } catch {
        case NonFatal(e) => LOG.warn(s"Download exception, URL: $urlStr", e)
      } finally {
        if (conn != null) conn.disconnect()
      }
    }
    throw new IllegalStateException(s"Data was not loaded, URL: $urlStr")
  }

  private def baseUrl = {
    System.getenv().getOrDefault("DATA_BASE_URL", "http://127.0.0.1:8000/")
  }

}

