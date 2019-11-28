package qnt

import java.io.{File, FileWriter}
import java.net.{HttpURLConnection, URL}
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.Scanner

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.saddle.index.OuterJoin
import org.saddle.{Frame, Index, Mat}
import org.slf4j.LoggerFactory
import ucar.nc2.NetcdfFile

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

object data {
  case class StockInfo (
                         id: String,
                         cik: Option[String],
                         exchange: String,
                         symbol: String,
                         name: String,
                         sector: Option[String],
                         props: Map[String, Any]
                       ) {

    def this(props: Map[String, Any]) = this(
      getClientId(
        props("id").asInstanceOf[String],
        props("exchange").asInstanceOf[String],
        props("symbol").asInstanceOf[String]
      ),
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
    lst.sortBy(i => i.getOrElse("last_point_id", "1900-01-01").asInstanceOf[String])
      .reverse
      .map(p => new StockInfo(p))
      .sortBy(i => i.id)
  }

  def loadStockDailySeries(
    ids: Seq[String],
    minDate: LocalDate = LocalDate.of(2007, 1, 1),
    maxDate: LocalDate = LocalDate.now()
  ) : Map[String, Frame[LocalDate, String, Double]] = {
    var series = loadStockDailyOriginSeries(ids, minDate, maxDate)
    // fix series by splits
    series = series.entrySet().map(e => e.getKey match {
      case Fields.vol => (e.getKey, e.getValue / series(Fields.split_cumprod))
      case Fields.open | Fields.low | Fields.high | Fields.close | Fields.divs
        => (e.getKey, e.getValue * series(Fields.split_cumprod))
      case _ => (e.getKey, e.getValue)
    }).toMap
    Runtime.getRuntime.gc()
    series
  }

  def loadStockDailyOriginSeries(
                            ids: Seq[String],
                            minDate: LocalDate = LocalDate.of(2007, 1, 1),
                            maxDate: LocalDate = LocalDate.now()
                          ) : Map[String, Frame[LocalDate, String, Double]] = {
    val serverIds = ids.map(getServerId).sorted

    var result = loadStockDailyRawSeries(serverIds, minDate, maxDate)

    val clientIds = result(Fields.close).colIx.map(i => getClientId(i))
    val sortedClientIds = clientIds.toVec.contents.sorted
    val timeList = result(Fields.close).rowIx.toVec.contents.sorted
    result = result.entrySet()
      .map(e => (e.getKey, e.getValue.setColIndex(clientIds).apply(timeList, sortedClientIds)))
      .toMap

    Runtime.getRuntime.gc()

    result
  }

  def loadIndexList() = ???

  def loadIndexSeries() = ???

  implicit private val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
  private val LOG = LoggerFactory.getLogger(getClass)
  private val RETRIES = 5
  private val TIMEOUT = 60*1000
  private val OBJECT_MAPPER = new ObjectMapper() with ScalaObjectMapper
  private val BATCH_LIMIT = 300000

  OBJECT_MAPPER.registerModule(DefaultScalaModule)

  def loadStockDailyRawSeries(
                                  ids: Seq[String],
                                  minDate: LocalDate = LocalDate.of(2007, 1, 1),
                                  maxDate: LocalDate = LocalDate.now()
                                ) : Map[String, Frame[LocalDate, String, Double]] = {
    var days = math.max(1, ChronoUnit.DAYS.between(minDate, maxDate).asInstanceOf[Int])
    var maxChunkSize = BATCH_LIMIT / days

    var offset = 0
    val chunks = ListBuffer[Map[String, Frame[LocalDate, String, Double]]]()
    while (offset < ids.length) {
      val size = math.min(maxChunkSize, ids.length - offset)
      LOG.info(s"Load chunk offset: offset=$offset size=$size length=${ids.length} maxChunkSize=$maxChunkSize")
      val chunkIds = ids.slice(offset, offset + size)
      val chunk = loadStockDailyRawChunk(chunkIds, minDate, maxDate)
      chunks += chunk
      offset += size
    }
    mergeStockDailyRawChunks(chunks)
  }

  private def mergeStockDailyRawChunks(chunks: Seq[Map[String, Frame[LocalDate, String, Double]]])
    :Map[String, Frame[LocalDate, String, Double]] = {
    var result = Map[String, Frame[LocalDate, String, Double]]()
    for(field <- Fields.values) {
      var frame = chunks.map(c => c(field)).reduce((f1, f2)=>f1.joinPreserveColIx(f2, OuterJoin))
      result  += (field -> frame)
    }
    result
  }

  private def loadStockDailyRawChunk(
                                               ids: Seq[String],
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

    val timeIdx = Index[LocalDate](timeArray)
    val assetIdx = Index[String](assetArray)
    val valueMatrices = values.grouped(timeArray.length*assetArray.length)
      .map(g => Mat(timeArray.length, assetArray.length, g))
      .toArray

    var result = Map[String, Frame[LocalDate, String, Double]]()

    for(fi <- fieldArray.indices) {
      val field = fieldArray(fi)
      val mat = valueMatrices(fi)
      val frame = Frame[LocalDate, String, Double](mat, timeIdx, assetIdx)
      result += (field -> frame)
    }
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

  private val clientToServerIdMapping = mutable.HashMap[String,String]()
  private val serverToClientIdMapping = mutable.HashMap[String,String]()
  private val idMappingFile = new File("id-translation.csv")

  if(idMappingFile.exists()) {
    val scanner = new Scanner(idMappingFile)
    var first = true
    while(scanner.hasNext()) {
      var line = scanner.nextLine()
      if(first) {
        first = false
      } else {
        line = line.strip()
        if(line.length > 2) {
          val parts = line.split(",")
          val serverId = parts(0)
          val clientId = parts(1)
          clientToServerIdMapping(clientId) = serverId
          serverToClientIdMapping(serverId) = clientId
        }
      }
    }
    scanner.close()
  } else {
    val writer = new FileWriter(idMappingFile)
    writer.write("server_id,client_id\n")
    writer.close()
  }

  private def getClientId(serverId: String, exchange: String = null, symbol: String = null): String = {
    if(serverToClientIdMapping.contains(serverId)) {
      serverToClientIdMapping(serverId)
    } else {
      if (exchange == null || symbol == null) {
        serverId
      } else {
        val preferedId = s"$exchange:$symbol"
        var clientId = preferedId
        var num = 0
        while (clientToServerIdMapping.contains(clientId)) {
          num += 1
          clientId = s"$preferedId~$num"
        }
        clientToServerIdMapping(clientId) = serverId
        serverToClientIdMapping(serverId) = clientId
        val writer = new FileWriter(idMappingFile)
        writer.write(s"$serverId,$clientId\n")
        writer.close()
        clientId
      }
    }
  }

  private def getServerId(clientId: String)
  : String = if(clientToServerIdMapping.contains(clientId)) clientToServerIdMapping(clientId) else clientId

}

