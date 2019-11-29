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


  def loadStockDailySeries(
    ids: Seq[String],
    minDate: LocalDate = LocalDate.of(2007, 1, 1),
    maxDate: LocalDate = LocalDate.now()
  ) : Map[String, Frame[LocalDate, String, Double]] = {
    var series = loadStockDailyOriginSeries(ids, minDate, maxDate)
    // fix series by splits
    series = series.entrySet().map(e => e.getKey match {
      case fields.vol => (e.getKey, e.getValue / series(fields.split_cumprod))
      case fields.open | fields.low | fields.high | fields.close | fields.divs
        => (e.getKey, e.getValue * series(fields.split_cumprod))
      case _ => (e.getKey, e.getValue)
    }).toMap
    Runtime.getRuntime.gc()
    series
  }

  object fields {
    val open = "open"
    val low = "low"
    val high = "high"
    val close = "close"
    val vol = "vol"
    val divs = "divs"
    val split = "split"
    val split_cumprod = "split_cumprod"
    val is_liquid = "is_liquid"

    val values = Set(open, low, high, close, vol, divs, split, split_cumprod, is_liquid )
  }

  def loadStockDailyOriginSeries(
                            ids: Seq[String],
                            minDate: LocalDate = LocalDate.of(2007, 1, 1),
                            maxDate: LocalDate = LocalDate.now()
                          ) : Map[String, Frame[LocalDate, String, Double]] = intern.ctx {
    val serverIds = ids.map(getServerId).sorted

    var result = loadStockDailyRawSeries(serverIds, minDate, maxDate)

    val clientIds = result(fields.close).colIx.map(i => getClientId(i))
    val sortedClientIds = clientIds.toVec.contents.sorted
    val timeList = result(fields.close).rowIx.toVec.contents.sorted
    result = result.entrySet()
      .map(e => (e.getKey, e.getValue.setColIndex(clientIds).apply(timeList, sortedClientIds)))
      .toMap

    Runtime.getRuntime.gc()

    result
  }

  def loadIndexList(
                     minDate: LocalDate = LocalDate.of(2007, 1, 1),
                     maxDate: LocalDate = LocalDate.now()
                   ) : List[IndexInfo] = {
    val uri = s"idx/list?min_date=$minDate&max_date=$maxDate"
    val dataBytes = loadWithRetry(uri)
    OBJECT_MAPPER.readValue[List[IndexInfo]](dataBytes)
  }

  case class IndexInfo (id: String, name: String) {
    def this(props: Map[String, String]) = this(props("id"), props("name"))
  }

  def loadIndexDailySeries(
                          ids: Seq[String],
                          minDate: LocalDate = LocalDate.of(2007, 1, 1),
                          maxDate: LocalDate = LocalDate.now()
                          ): Frame[LocalDate, String, Double] = {
    val params = Map("ids" -> ids, "min_date"-> minDate.toString, "max_date"-> maxDate.toString)
    val resBytes = loadWithRetry("idx/data", params)
    val resFrame = netcdf2DToFrames(resBytes)
    resFrame.apply(resFrame.rowIx.toSeq.toArray.sorted, resFrame.colIx.toSeq.toArray.sorted)
  }


  def loadSecgovForms(
                         ciks: Seq[String],
                         types: Seq[String] = null,
                         facts: Seq[String] = null,
                         skipSegment: Boolean = true,
                         minDate: LocalDate = LocalDate.of(2007, 1, 1),
                         maxDate: LocalDate = LocalDate.now()
                       ): List[SecForm] = intern.ctx {
    val lst = mutable.ListBuffer[SecForm]()
    var go = true
    var offset:Int = 0
    val params = mutable.HashMap[String, Any](
      "ciks" -> ciks,
      "offset" -> offset,
      "facts" -> facts,
      "skip_segment" -> skipSegment,
      "min_date" -> minDate.toString,
      "max_date" -> maxDate.toString
    )
    while (go) {
      val bytes = loadWithRetry("sec.gov/forms", params)
      val p = OBJECT_MAPPER.readValue[List[Map[String,Any]]](bytes)
      p.foreach(r => lst.add(new SecForm(r)))
      offset += p.length
      params("offset") = offset
      LOG.info(s"fetched reports ${lst.length} (+${p.length})")
      go = p.nonEmpty
    }
    Runtime.getRuntime.gc()
    lst.toList
  }

  case class SecForm(cik: String, date: LocalDate, formType:String, url: String, facts: Set[Fact]) {
    def this(props: Map[String,Any]) = this(
      intern(props("cik").asInstanceOf[String].intern()),
      intern(LocalDate.parse(props("date").asInstanceOf[String])),
      intern(props("type").asInstanceOf[String].intern()),
      props("url").asInstanceOf[String],
      props("facts").asInstanceOf[List[Map[String, Any]]].map(i => new Fact(i)).toSet
    )
  }

  object SecgovFormTypes {
    val f10k = "10-K"
    val f10q = "10-Q"

    val values = Set(f10k, f10q)
  }

  /**

    * @param period - LocalTime or RangePeriod or ForeverPeriod or null
    * @param unit - MeasureUnit or DivideUnit or null
    * @param value - String or Double or Int or Long or null
    */
  case class Fact(
                   name:String,
                   segment: Any ,
                   period: Any ,
                   unit: Any,

                   exact: Boolean,
                   haveDecimal: Boolean,
                   decimals: Int,
                   havePrecision: Boolean,
                   precision: Int,

                   value:Any
                 ){


    def this(props: Map[String, Any]) = this(
      intern(props("name").asInstanceOf[String]),
      intern(props("segment")),
      intern(convertPeriod(props("period"))),
      intern(convertUnit(props("unit"))),
      props("decimals") == "INF" || props("precision") == "INF",
      props("decimals") != null,
      props("decimals") match {
        case null | "INF" => 0
        case _ => props("decimals").asInstanceOf[Int]
      },
      props("precision") != null,
      props("precision") match {
        case null | "INF" => 0
        case _ => props("precision").asInstanceOf[Int]
      } ,
      props("value")
    )
  }

  case class MeasureUnit(measure:String)  {}
  case class DivideUnit(numerator:String, denominator:String)  {}
  case class RangePeriod(start: LocalDate, end: LocalDate)
  object foreverPeriod {}

  private def convertPeriod(v:Any):Any = {
    if(v.isInstanceOf[Map[_,_]]) {
      val m = v.asInstanceOf[Map[String, Any]]
      m("type") match {
        case "instant" => intern(LocalDate.parse(m("value").asInstanceOf[String]))
        case "range" => {
          val r = m("value").asInstanceOf[List[String]].map(i => intern(LocalDate.parse(i)))
          intern(RangePeriod(r(0), r(1)))
        }
        case "forever" => foreverPeriod
        case _ => null
      }
    } else {
      null
    }
  }

  private def convertUnit(v:Any):Any = {
    if(v.isInstanceOf[Map[_,_]]) {
      val m = v.asInstanceOf[Map[String, Any]]
      m("type") match {
        case "measure" => intern(MeasureUnit(intern(m("value").asInstanceOf[String])))
        case "divide" =>
          val r = m("value").asInstanceOf[List[String]].map(intern(_))
          intern(DivideUnit(r(0),r(1)))
        case _ => null
      }
    } else {
      null
    }
  }

  private val LOG = LoggerFactory.getLogger(getClass)
  private val RETRIES = if(System.getenv().contains("SUBMISSION_ID")) Int.MaxValue else 5
  private val TIMEOUT = 60*1000
  private val OBJECT_MAPPER = new ObjectMapper() with ScalaObjectMapper
  OBJECT_MAPPER.registerModule(DefaultScalaModule)
  private val BATCH_LIMIT = 300000

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
    for(field <- fields.values) {
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
    netcdf3DToFrames(dataBytes)
  }

  private def netcdf3DToFrames(bytes: Array[Byte]): Map[String, Frame[LocalDate, String, Double]] = {
    val dataNetcdf = NetcdfFile.openInMemory("data", bytes)

    val vars = dataNetcdf.getVariables.toList.map(v=>(v.getShortName, v)).toMap

    val timeVar = vars("time")
    val zeroDateStr = timeVar.getAttributes.toList.filter(i=>i.getShortName == "units").get(0).getStringValue
    val zeroDate = LocalDate.parse(zeroDateStr.split(" since ")(1).split(" ")(0))
    val timeRawArray = dataNetcdf.readSection("time").copyTo1DJavaArray().asInstanceOf[Array[Int]]
    val timeArray = timeRawArray.map(i => intern(zeroDate.plusDays(i)))

    var fieldVar = vars("field")
    val fieldRawArray = dataNetcdf.readSection("field").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val fieldArray = fieldRawArray.map(i => intern(new String(i.filter(c => c != '\0'))))

    val assetVar = vars("asset")
    val assetRawArray = dataNetcdf.readSection("asset").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val assetArray = assetRawArray.map(i => intern(new String(i.filter(c => c != '\0'))))

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

  private def netcdf2DToFrames(bytes: Array[Byte]): Frame[LocalDate, String, Double] = {
    val dataNetcdf = NetcdfFile.openInMemory("data", bytes)

    val vars = dataNetcdf.getVariables.toList.map(v=>(v.getShortName, v)).toMap

    val timeVar = vars("time")
    val zeroDateStr = timeVar.getAttributes.toList.filter(i=>i.getShortName == "units").get(0).getStringValue
    val zeroDate = LocalDate.parse(zeroDateStr.split(" since ")(1).split(" ")(0))
    val timeRawArray = dataNetcdf.readSection("time").copyTo1DJavaArray().asInstanceOf[Array[Int]]
    val timeArray = timeRawArray.map(i => zeroDate.plusDays(i))

    val assetVar = vars("asset")
    val assetRawArray = dataNetcdf.readSection("asset").copyToNDJavaArray().asInstanceOf[Array[Array[Char]]]
    val assetArray = assetRawArray.map(i => new String(i.filter(c => c != '\0')))

    // C-order of dimensions: time,asset
    val values = dataNetcdf.readSection("__xarray_dataarray_variable__").copyTo1DJavaArray().asInstanceOf[Array[Double]]

    val timeIdx = Index[LocalDate](timeArray)
    val assetIdx = Index[String](assetArray)
    val valueMatrix = Mat(timeArray.length, assetArray.length, values)

    var result = Map[String, Frame[LocalDate, String, Double]]()

    Frame[LocalDate, String, Double](valueMatrix, timeIdx, assetIdx)
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
        val writer = new FileWriter(idMappingFile, true)
        writer.write(s"$serverId,$clientId\n")
        writer.close()
        clientId
      }
    }
  }

  private def getServerId(clientId: String)
  : String = if(clientToServerIdMapping.contains(clientId)) clientToServerIdMapping(clientId) else clientId

  implicit private val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  private object intern {
    var internMap : mutable.HashMap[Any,Any] = null
    var miss = 0
    var hit = 0

    def ctx[R](fun:  => R) = {
      if(internMap != null) {
        fun
      } else {
        try{
          hit = 0
          miss = 0
          internMap = mutable.HashMap[Any,Any]()
          fun
        } finally {
          internMap.clear()
          internMap = null
          LOG.info(s"intern hit = $hit, miss= $miss")
        }
      }
    }

    def apply[T](value : T):T = {
      if(internMap == null) {
        miss += 1
        value
      }else {
        if(internMap.contains(value)) {
          hit += 1
          internMap(value).asInstanceOf[T]
        } else {
          miss += 1
          internMap(value) = value
          value
        }
      }
    }
  }
}

