package utils

import java.util.UUID

import scala.util.Try

object StringParserUtil {

  def columnDefinitions(columns: String): List[(String, String)] = {
    val columnsTuple: List[(String, String)] = columns.split(",")
      .map { col => {
        val elements: Array[String] = col.trim.split(" ")
        val colName: String = Try{elements(0)}.getOrElse("")
        val colType: String = Try{elements(1)}.getOrElse("")
        (colName, colType)
        }
      }.toList.filterNot(col => col._2 == "")
    columnsTuple
  }

  def validateIdFormat(id: Option[String]): Boolean = {
    id match {
      case Some(dslId) => {
        UUID.fromString(dslId)
        true
      }
      case _ => false
    }
  }


}
