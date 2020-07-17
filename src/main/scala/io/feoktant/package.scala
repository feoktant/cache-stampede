package io

import scredis.serialization.{Reader, UTF8StringWriter, Writer}

package object feoktant {

  implicit object Tuple2Writer extends Writer[(Int, Long)] with Reader[(Int, Long)] {

    override def writeImpl(t: (Int, Long)): Array[Byte] =
      UTF8StringWriter.write(s"${t._1}:${t._2}")

    override protected def readImpl(bytes: Array[Byte]): (Int, Long) = {
      val t = scredis.serialization.UTF8StringReader.read(bytes).split(':')
      (t(0).toInt, t(1).toLong)
    }
  }
}
