name := "adapter-esper"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.11"

libraryDependencies ++= Seq(
  //"es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.9" exclude("org.slf4j","slf4j-log4j12"),
  "es.upm.fi.oeg.morph.streams" % "esper-engine" % "1.0.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"  
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

