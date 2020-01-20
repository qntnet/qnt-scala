package qnt

import org.slf4j.LoggerFactory

import scala.collection.mutable

object intern {
  private val LOG = LoggerFactory.getLogger(getClass)
  private var internMap : mutable.HashMap[Any,Any] = null
  private var miss = 0
  private var hit = 0

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
