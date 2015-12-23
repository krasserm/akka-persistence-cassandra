package akka.persistence.cassandra

import java.io.{File, FileInputStream}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory, TrustManager, KeyManager}
import scala.collection.immutable.Seq

private [cassandra] object SSLSetup {
  /**
   * creates a new SSLContext
   */
  def constructContext(
    trustStorePath:Option[String],
    trustStorePW:Option[String],
    keyStorePath:Option[String],
    keyStorePW:Option[String]):SSLContext = {

    val ctx = SSLContext.getInstance("SSL")   
    ctx.init(
      getKeyManagers(keyStorePath, keyStorePW).toArray,
      getTrustManagers(trustStorePath, trustStorePW).toArray, 
      new SecureRandom())
    ctx
  }

  def loadKeyStore(
    storePath:String,
    storePassword:String):KeyStore = {
    val ks = KeyStore.getInstance("JKS")
    val f = new File(storePath)
    if(!f.isFile) throw new IllegalArgumentException(s"JKSs path $storePath not found.")
    val is = new FileInputStream(f)

    try {
      ks.load(is, storePassword.toCharArray)
    } finally (is.close())

    ks
  }

  def getTrustManagers(
    trustStorePath:Option[String],
    trustStorePassword:Option[String]):Seq[TrustManager] = {
    
    trustStorePath.toList.flatMap{ path =>
      val ts = loadKeyStore(path, trustStorePassword.getOrElse("changeit"))
      val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      tmf.init(ts)
      tmf.getTrustManagers
    }
  }

  def getKeyManagers(
    keyStorePath:Option[String],
    keyStorePassword:Option[String]):Seq[KeyManager] = {

    keyStorePath.toList.flatMap { path =>
      val password = keyStorePassword.getOrElse("changeit")
      val ks = loadKeyStore(path, password)
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(ks, password.toCharArray)
      kmf.getKeyManagers
    }
  }
}