import org.specs2.mutable.SpecificationWithJUnit
import utils.StringParserUtil

class UUIDStringParserUtilTest extends SpecificationWithJUnit {

  val testColumns1 : Option[String] = Some("348c2c80-4dd3-11e8-9048-a3c585d1d249")
  val testColumns1a : Option[String] = Some("01234567-0123-0123-0123-0123456789ab")

  val testColumns2 : Option[String] = Some("348c2c80-4dd3-11e8-9043-a3c585d1d249")

  val testColumns3 : Option[String] = Some("348c2c80-4dd3-11e8-f048-a3c585d1d24")

  val testColumns4 : Option[String] = None

  "UUID parser" should {

    "valid input 1" in {
      StringParserUtil.validateIdFormat(testColumns1)
    }

    "valid input 1a" in {
      StringParserUtil.validateIdFormat(testColumns1a)
    }

    "valid input 2" in {
      StringParserUtil.validateIdFormat(testColumns2)
    }

    "invalid input 1" in {
      StringParserUtil.validateIdFormat(testColumns3)
    }

    "invalid input 2" in {
      StringParserUtil.validateIdFormat(testColumns4) === false
    }
  }


}
