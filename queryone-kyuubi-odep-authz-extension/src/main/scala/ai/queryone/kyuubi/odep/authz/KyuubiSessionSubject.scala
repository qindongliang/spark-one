package ai.queryone.kyuubi.odep.authz

import org.apache.spark.SparkContext

import java.nio.charset.StandardCharsets
import java.security.{KeyFactory, Signature}
import java.security.spec.X509EncodedKeySpec
import java.util.Base64

private[authz] object KyuubiSessionSubject {
  private val SessionUser = "kyuubi.session.user"
  private val SessionPublicKey = "kyuubi.session.sign.publickey"
  private val SessionUserSign = "kyuubi.session.user.sign"
  private val SessionUserSignEnabled = "spark.kyuubi.session.user.sign.enabled"

  def resolve(sparkContext: SparkContext): String = {
    if (!sparkContext.getConf.getBoolean(SessionUserSignEnabled, false)) {
      throw new OdepAuthorizationException(
        s"$SessionUserSignEnabled must be enabled for ODEP authorization")
    }

    val user = requiredLocalProperty(sparkContext, SessionUser)
    val publicKey = requiredLocalProperty(sparkContext, SessionPublicKey)
    val signature = requiredLocalProperty(sparkContext, SessionUserSign)
    if (!verify(user, publicKey, signature)) {
      throw new OdepAuthorizationException("Invalid Kyuubi session user signature")
    }
    user
  }

  private[authz] def verify(user: String, publicKeyBase64: String, signatureBase64: String): Boolean = {
    try {
      val publicKey = KeyFactory.getInstance("EC").generatePublic(
        new X509EncodedKeySpec(Base64.getDecoder.decode(publicKeyBase64)))
      val verifier = Signature.getInstance("SHA256withECDSA")
      verifier.initVerify(publicKey)
      verifier.update(user.getBytes(StandardCharsets.UTF_8))
      verifier.verify(Base64.getDecoder.decode(signatureBase64))
    } catch {
      case _: Exception => false
    }
  }

  private def requiredLocalProperty(sparkContext: SparkContext, name: String): String = {
    val value = sparkContext.getLocalProperty(name)
    if (value == null || value.trim.isEmpty) {
      throw new OdepAuthorizationException(s"Missing trusted Kyuubi session property: $name")
    }
    value
  }
}
