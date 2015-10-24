package akka.persistence.cassandra

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{SSLOptions, QueryOptions, Cluster}

import scala.collection.JavaConverters._

object ClusterBuilder {

  def cluster(cassandraPluginConfig: CassandraPluginConfig) = {
    import cassandraPluginConfig._

    val clusterBuilder: Cluster.Builder = Cluster.builder
      .addContactPointsWithPorts(contactPoints.asJava)
      .withQueryOptions(new QueryOptions().setFetchSize(fetchSize))

    if (config.hasPath("authentication")) {
      clusterBuilder.withCredentials(
        config.getString("authentication.username"),
        config.getString("authentication.password"))
    }

    if (config.hasPath("local-datacenter")) {
      clusterBuilder.withLoadBalancingPolicy(
        new TokenAwarePolicy(
          new DCAwareRoundRobinPolicy(config.getString("local-datacenter"))
        )
      )
    }

    if (config.hasPath("ssl")) {
      val trustStorePath: String = config.getString("ssl.truststore.path")
      val trustStorePW: String = config.getString("ssl.truststore.password")
      val keyStorePath: String = config.getString("ssl.keystore.path")
      val keyStorePW: String = config.getString("ssl.keystore.password")

      val context = SSLSetup.constructContext(
        trustStorePath,
        trustStorePW,
        keyStorePath,
        keyStorePW)

      clusterBuilder.withSSL(new SSLOptions(context, SSLOptions.DEFAULT_SSL_CIPHER_SUITES))
    }

    clusterBuilder.build
  }
}
