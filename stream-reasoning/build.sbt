name := "stream-reasoning"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.2"

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph" % "kyrie" % "0.18.1",  
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "nightlabs" at "http://dev.nightlabs.org/maven-repository/repo"
  //"jmora" at "https://dl.dropboxusercontent.com/u/452942/maven"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")





