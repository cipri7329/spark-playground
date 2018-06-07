import org.specs2.mutable.SpecificationWithJUnit
import utils.StringParserUtil

class StringParserUtilTest extends SpecificationWithJUnit {

  val testColumns1 : String =
  s"""
     |solutionColumn text,
     |userIdColumn text,
     |idColumnName timeuuid,
     |expressionColumnName text,
     |duration bigint,
     |error text,
     |messageIdColumnName text,
     |startedat timestamp,
     |finishedat timestamp,
     |status text,
     |parentExecutionId text
    """.stripMargin.replaceAll("\n", "")

  val testColumns2 : String =
    s"""
       |solutionColumn text,
       |userIdColumn text,
       |idColumnName timeuuid,
       |expressionColumnName text,
       |duration bigint,
       |error text,
       |messageIdColumnName text,
       |startedat timestamp,
       |finishedat timestamp,
       |status text,
       |parentExecutionId text,
    """.stripMargin.replaceAll("\n", "")

  val testColumns3 : String =
    s"""
       |solutionColumn_text,
       |userIdColumn text,
       |idColumnName timeuuid,
       |expressionColumnName text,
       |duration bigint,
       |error text,
       |messageIdColumnName text,
       |startedat timestamp,
       |finishedat timestamp,
       |status text,
       |parentExecutionId text,
    """.stripMargin.replaceAll("\n", "")

  val testColumns4 : String = "solution text, internal_name text, descriptor blob, descriptor_raw text"

  "columns parser" should {

    "parse columns" in {
      val colDefs = StringParserUtil.columnDefinitions(testColumns1)
      colDefs.length === 11
    }

    "parse columns - example 4" in {
      val colDefs: Seq[(String, String)] = StringParserUtil.columnDefinitions(testColumns4)
      colDefs.length === 4
      colDefs(0)._1 === "solution"
      colDefs(0)._2 === "text"

      colDefs(1)._1 === "internal_name"
      colDefs(1)._2 === "text"

      colDefs(2)._1 === "descriptor"
      colDefs(2)._2 === "blob"
    }

    "parse columns with extra comma" in {
      val colDefs = StringParserUtil.columnDefinitions(testColumns2)
      colDefs.length === 11
    }

    "parse columns with invalid entry" in {
      val colDefs = StringParserUtil.columnDefinitions(testColumns3)
      colDefs.length === 10
    }

  }


}
