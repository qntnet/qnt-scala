package ai.quantnet

import java.io.ByteArrayOutputStream
import java.net.{HttpURLConnection, URL}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object net {
  private val LOG = LoggerFactory.getLogger(getClass)
  private val RETRIES = if(System.getenv().keySet.contains("SUBMISSION_ID")) Int.MaxValue else 5
  private val TIMEOUT = 60*1000
  val OBJECT_MAPPER = new ObjectMapper() with ScalaObjectMapper
  OBJECT_MAPPER.registerModule(DefaultScalaModule)

  def httpRequestWithRetry(urlStr: String, jsonRequestObj: Any = null): Array[Byte] = {
    val url = new URL(urlStr)
    for (t <- 1 to RETRIES) {
      try {
        return httpRequest(urlStr, jsonRequestObj)
      } catch {
        case NonFatal(e) => LOG.warn(s"httpRequestWithRetry exception, retrying, URL: $urlStr", e)
      }
    }
    throw new IllegalStateException(s"httpRequestWithRetry failed, URL: $urlStr")
  }

  def httpRequest(urlStr: String, jsonRequestObj: Any = null): Array[Byte] = {
    val url = new URL(urlStr)
      var conn: HttpURLConnection = null
      try {
        conn = url.openConnection().asInstanceOf[HttpURLConnection]
        conn.setConnectTimeout(TIMEOUT)
        conn.setReadTimeout(TIMEOUT)
        conn.setDoOutput(jsonRequestObj != null)
        conn.setRequestMethod(if(jsonRequestObj == null) "GET" else "POST")
        conn.setUseCaches(false)
        conn.setDoInput(true)
        if(jsonRequestObj != null) {
          val dataBytes = OBJECT_MAPPER.writeValueAsBytes(jsonRequestObj)
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
        val result = new ByteArrayOutputStream()
        IOUtils.copy(is, result)
        result.toByteArray
      } finally {
        if (conn != null) conn.disconnect()
      }
  }

}
