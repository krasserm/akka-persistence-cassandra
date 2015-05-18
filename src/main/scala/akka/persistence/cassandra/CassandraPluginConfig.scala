package akka.persistence.cassandra

import java.io.{File, FileInputStream, InputStream}
import java.net.InetSocketAddress
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.{Cluster, ConsistencyLevel, SSLOptions}
import com.typesafe.config.Config

import scala.collection.JavaConverters._


class CassandraPluginConfig(config: Config) {

  import akka.persistence.cassandra.CassandraPluginConfig._

  val keyspace: String = config.getString("keyspace")
  val table: String = config.getString("table")

  val keyspaceAutoCreate: Boolean = config.getBoolean("keyspace-autocreate")
  val keyspaceAutoCreateRetries: Int = config.getInt("keyspace-autocreate-retries")

  val replicationStrategy: String = getReplicationStrategy(
    config.getString("replication-strategy"),
    config.getInt("replication-factor"),
    config.getStringList("data-center-replication-factors").asScala)

  val readConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("read-consistency"))
  val writeConsistency: ConsistencyLevel = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val port: Int = config.getInt("port")
  val contactPoints = getContactPoints(config.getStringList("contact-points").asScala, port)

  val clusterBuilder: Cluster.Builder = Cluster.builder
    .addContactPointsWithPorts(contactPoints.asJava)

  if (config.hasPath("authentication")) {
    clusterBuilder.withCredentials(
      config.getString("authentication.username"),
      config.getString("authentication.password"))
  }

  if (config.hasPath("local-datacenter")) {
    clusterBuilder.withLoadBalancingPolicy(
      new DCAwareRoundRobinPolicy(config.getString("local-datacenter"))
    )
  }

  if(config.hasPath("ssl")) {
    val trustStore: InputStream = new FileInputStream(new File(config.getString("ssl.truststore.path")))
    val trustStorePW: String = config.getString("ssl.truststore.password")
    val keyStore: InputStream = new FileInputStream(new File(config.getString("ssl.keystore.path")))
    val keyStorePW: String = config.getString("ssl.keystore.password")
    val context:SSLContext = getContext(trustStore,trustStorePW,keyStore,keyStorePW)
    clusterBuilder.withSSL(new SSLOptions(context,SSLOptions.DEFAULT_SSL_CIPHER_SUITES))
  }
}

object CassandraPluginConfig {

  /**
   * Builds list of InetSocketAddress out of host:port pairs or host entries + given port parameter.
   */
  def getContactPoints(contactPoints: Seq[String], port: Int): Seq[InetSocketAddress] = {
    contactPoints match {
      case null | Nil => throw new IllegalArgumentException("A contact point list cannot be empty.")
      case hosts => hosts map {
        ipWithPort => ipWithPort.split(":") match {
          case Array(host, port) => new InetSocketAddress(host, port.toInt)
          case Array(host) => new InetSocketAddress(host, port)
          case msg => throw new IllegalArgumentException(s"A contact point should have the form [host:port] or [host] but was: $msg.")
        }
      }
    }
  }

  /**
   * Builds replication strategy command to create a keyspace.
   */
  def getReplicationStrategy(strategy: String, replicationFactor: Int, dataCenterReplicationFactors: Seq[String]): String = {

    def getDataCenterReplicationFactorList(dcrfList: Seq[String]): String = {
      val result: Seq[String] = dcrfList match {
        case null | Nil => throw new IllegalArgumentException("data-center-replication-factors cannot be empty when using NetworkTopologyStrategy.")
        case dcrfs => dcrfs.map {
          dataCenterWithReplicationFactor => dataCenterWithReplicationFactor.split(":") match {
            case Array(dataCenter, replicationFactor) => s"'$dataCenter':$replicationFactor"
            case msg => throw new IllegalArgumentException(s"A data-center-replication-factor must have the form [dataCenterName:replicationFactor] but was: $msg.")
          }
        }
      }
      result.mkString(",")
    }

    strategy.toLowerCase() match {
      case "simplestrategy" => s"'SimpleStrategy','replication_factor':$replicationFactor"
      case "networktopologystrategy" => s"'NetworkTopologyStrategy',${getDataCenterReplicationFactorList(dataCenterReplicationFactors)}"
      case unknownStrategy => throw new IllegalArgumentException(s"$unknownStrategy as replication strategy is unknown and not supported.")
    }
  }

  /**
   * creates a new SSLContext
   */
  def getContext(truststore:InputStream,
                 truststorePassword:String,
                 keystore:InputStream,
                 keystorePassword:String):SSLContext = {
    val ctx = SSLContext.getInstance("SSL")
    val ts = KeyStore.getInstance("JKS")
    ts.load(truststore, truststorePassword.toCharArray)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    val ks = KeyStore.getInstance("JKS")
    ks.load(keystore, keystorePassword.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, keystorePassword.toCharArray)
    ctx.init(kmf.getKeyManagers, tmf.getTrustManagers, new SecureRandom())
    ctx
  }
}
