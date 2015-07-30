package akka.persistence.cassandra

import com.typesafe.config.ConfigFactory

object TestConfig {

  val config = ConfigFactory.parseResources("application-test.conf")
}
