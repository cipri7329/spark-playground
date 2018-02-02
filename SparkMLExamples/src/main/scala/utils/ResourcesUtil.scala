package utils

import java.nio.file.Paths

/**
  *
  */
object ResourcesUtil {

  def resourcePath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString
}
