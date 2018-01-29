package utils

import scala.util.Try
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}

object DataframeSchemaUtil {

  /** Produce a Schema string from a Dataset */
  def serializeSchema(ds: Dataset[_]): String = ds.schema.json

  /** Produce a StructType schema object from a JSON string */
  def deserializeSchema(json: String): StructType = {
    Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
      case t: StructType => t
      case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
    }
  }

}
