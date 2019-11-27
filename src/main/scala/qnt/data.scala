package qnt

import java.net.{HttpURLConnection, URL}
import java.time.LocalDate

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.saddle.Frame
import org.slf4j.LoggerFactory
import ucar.ma2.ArrayDouble
import ucar.nc2.NetcdfFile

import scala.collection.immutable.{Map => ImmutableMap}
import scala.util.control.NonFatal

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

object data {

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
                          ) : Frame[LocalDate, String, Float] = {
    var uri = "data"
    var params = Map(
      "assets" -> ids,
      "min_date" -> minDate.toString,
      "max_date" -> maxDate.toString
    )
    val  ad: ArrayDouble = null;

    var dataBytes = loadWithRetry(uri, params)
    val dataNetcdf = NetcdfFile.openInMemory("data", dataBytes)
    null
  }

  private val LOG = LoggerFactory.getLogger(getClass)
  private val RETRIES = 5
  private val TIMEOUT = 60*1000
  private val OBJECT_MAPPER = new ObjectMapper() with ScalaObjectMapper

  OBJECT_MAPPER.registerModule(DefaultScalaModule)

  def loadIndexList() = ???

  def loadIndexSeries() = ???

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
        conn.setDoInput(true);
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

