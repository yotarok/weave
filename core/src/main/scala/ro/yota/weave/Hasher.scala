package ro.yota.weave

import java.security.MessageDigest
import scalax.file.Path
import java.nio.charset.StandardCharsets
import com.typesafe.scalalogging.Logger

object Hasher {

  final val SHA1_HEX_LENGTH = 40

  private[this] val hex: Array[String] = (for (b <- 0 to 255) yield { f"$b%02x"}).toArray

  /**
    * Value cache for `apply(String)` and `apply(Array[Byte])` functions
    *
    * Since MessageDigest is mutable, it's unsafe to have a cache.
    * However, while the access is restricted to those functions,
    * we can benefit from reducing the function-call overhead.
    */
  private[this] val sha1 = MessageDigest.getInstance("SHA-1")

  /**
    * Hash the given string and returns hex string repr
    *
    * This function is actually an important key for performance.
    * Keep it as fast as possible!!
    */
  @inline def apply(s: String): String =
    sha1.digest(s.getBytes(StandardCharsets.UTF_8)).map(b => hex(b & 0xFF)).mkString

  @inline def apply(bs: Array[Byte]): String =
    sha1.digest(bs).map(b => hex(b & 0xFF)).mkString

  final private val HASHER_BATCH_SIZE: Int = 65536

  private[this] val defaultLogger = Logger("Hasher")

  def apply(path: Path)(implicit logger: Logger = defaultLogger): String = {
    logger.debug(s"Computing hash for ${path.path} (size=${path.size})...")
    val hasher = MessageDigest.getInstance("SHA-1")
    for (block <- path.bytes.grouped(HASHER_BATCH_SIZE)) {
      val chunk = block.toSeq
      hasher.update(chunk.toArray)
    }
    val ret = hasher.digest().map(b=>hex(b & 0xFF)).mkString("")
    logger.debug(s"Hash computation. done. (${ret})")
    ret
  }


}

