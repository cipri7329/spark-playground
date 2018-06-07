package utils.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.{ClassTag, _}

object JsonUtil {
  val jacksonMapper = new ObjectMapper()
  jacksonMapper.registerModule(DefaultScalaModule)
  //mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    jacksonMapper.writeValueAsString(value)
  }

  def fromJson[T: ClassTag](json: String): T = {
    jacksonMapper.readValue[T](json, classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }


}
