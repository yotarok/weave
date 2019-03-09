package ro.yota.weave

final case class ConfigError(
  private val message: String,
  private val cause: Throwable = None.orNull
) extends Exception(message, cause)

