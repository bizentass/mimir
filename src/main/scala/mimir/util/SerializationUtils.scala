package mimir.util

object SerializationUtils {

  private val base64in = java.util.Base64.getDecoder()
  private val base64out = java.util.Base64.getEncoder()

  def serialize[A](obj:A):Array[Byte] =
  {
    val out = new java.io.ByteArrayOutputStream()
    val objects = new java.io.ObjectOutputStream(out)
    objects.writeObject(obj)
    objects.flush();
    out.toByteArray();
  }
  def serializeToBase64[A](obj:A): String =
  {
    b64encode(serialize(obj))
  }

  def deserialize[A](data: Array[Byte]): A =
  {
    val objects = new java.io.ObjectInputStream(
      new java.io.ByteArrayInputStream(data)
    )
    objects.readObject().asInstanceOf[A]
  }
  def deserializeFromBase64[A](data: String): A =
  {
    deserialize[A](b64decode(data))
  }

  def b64encode(data: Array[Byte]): String =
    base64out.encodeToString(data)
  def b64decode(data: String): Array[Byte] =
    base64in.decode(data)

}