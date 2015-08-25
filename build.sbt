organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

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

libraryDependencies ++= {
  val akkaV = "2.4.0-RC1"
  val cassandraV = "2.1.5"
  val scalatestV = "2.1.4"
  Seq(
    "com.datastax.cassandra"  % "cassandra-driver-core"             % cassandraV,
    "com.typesafe.akka"      %% "akka-persistence"                  % akkaV,
    "com.typesafe.akka"      %% "akka-persistence-tck"              % akkaV      % "test",
    "org.scalatest"          %% "scalatest"                         % scalatestV % "test",
    "org.cassandraunit"       % "cassandra-unit"                    % "2.0.2.2"  % "test"
  )
}

