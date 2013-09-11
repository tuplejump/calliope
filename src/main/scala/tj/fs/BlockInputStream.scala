package tj.fs

import java.io.{IOException, InputStream}
import tj.model.BlockMeta
import scala.concurrent.Await
import scala.concurrent.duration._

case class
BlockInputStream(store: FileSystemStore, blockMeta: BlockMeta) extends InputStream {
  private val LENGTH = blockMeta.length
  private val AT_MOST: FiniteDuration = 10 seconds

  private var isClosed: Boolean = false
  private var inputStream: InputStream = null
  private var currentPosition: Long = 0

  private var targetSubBlockSize = 0L
  private var targetSubBlockOffset = 0L

  private def findSubBlock(targetPosition: Long): InputStream = {
    val subBlockLengthTotals = blockMeta.subBlocks.scanLeft(0L)(_ + _.length).tail
    val subBlockIndex = subBlockLengthTotals.indexWhere(p => targetPosition < p)
    if (subBlockIndex == -1) {
      throw new IOException("Impossible situation: could not find position " + targetPosition)
    }
    var offset = targetPosition
    if (subBlockIndex != 0) {
      offset -= subBlockLengthTotals(subBlockIndex - 1)
    }
    val subBlock = blockMeta.subBlocks(subBlockIndex)
    targetSubBlockSize = subBlock.length
    targetSubBlockOffset = subBlock.offset

    Await.result(store.retrieveSubBlock(blockMeta.id, subBlock.id, offset), AT_MOST)
  }

  def read: Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    var result = -1
    if (currentPosition <= LENGTH - 1) {
      if (currentPosition > (targetSubBlockOffset + targetSubBlockSize - 1)) {
        if (inputStream != null) {
          inputStream.close()
        }
        inputStream = findSubBlock(currentPosition)
      }
      result = inputStream.read()
      currentPosition += 1
    }
    result
  }

  override def read(buf: Array[Byte], off: Int, len: Int): Int = {
    if (isClosed) {
      throw new IOException("Stream closed")
    }
    if (buf == null) {
      throw new NullPointerException
    }
    if ((off < 0) || (len < 0) || (len > buf.length - off)) {
      throw new IndexOutOfBoundsException
    }
    var result = 0
    if (len > 0) {
      while (result < len && currentPosition <= LENGTH - 1) {
        if (currentPosition > (targetSubBlockOffset + targetSubBlockSize - 1)) {
          if (inputStream != null) {
            inputStream.close()
          }
          inputStream = findSubBlock(currentPosition)
        }
        val remaining = len - result
        val size = math.min(remaining, targetSubBlockSize)
        val readSize = inputStream.read(buf, off + result, size.asInstanceOf[Int])
        result += readSize
        currentPosition += readSize
      }
      if (result == 0) {
        result = -1
      }
    }
    result
  }

  override def close() = {
    if (!isClosed) {
      if (inputStream != null) {
        inputStream.close()
      }
      super.close()
      isClosed = true
    }
  }
}
