package utils.serialization

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.{RequiredPropertiesSchemaModule, ScalaObjectMapper}

object JsonConfiguration {
  val JsonMapper = new ObjectMapper() with ScalaObjectMapper
  JsonMapper.registerModule(new DefaultScalaModule with RequiredPropertiesSchemaModule)
  JsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
  JsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  JsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
  JsonMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
}