organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.5-SNAPSHOT"

scalaVersion := "2.11.6"

fork in Test := true

javaOptions in Test += "-Xmx2500M"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  //"-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "2.1.8",
  "com.typesafe.akka"      %% "akka-persistence"                  % "2.4.0",
  "com.typesafe.akka"      %% "akka-persistence-tck"              % "2.4.0"      % "test",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"      % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "2.1.9.2"    % "test",
  "log4j"                   % "log4j"                             % "1.2.17"     % "test",
  "org.slf4j"               % "slf4j-log4j12"                     % "1.7.5"      % "test",
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "2.1.8"      % "test" classifier "tests"
)

credentials += Credentials(
  "Artifactory Realm",
  "oss.jfrog.org",
  sys.env.getOrElse("OSS_JFROG_USER", ""),
  sys.env.getOrElse("OSS_JFROG_PASS", "")
)

publishTo := {
  val jfrog = "https://oss.jfrog.org/artifactory/"
  if (isSnapshot.value)
    Some("OJO Snapshots" at jfrog + "oss-snapshot-local")
  else
    Some("OJO Releases" at jfrog + "oss-release-local")
}

publishMavenStyle := true
