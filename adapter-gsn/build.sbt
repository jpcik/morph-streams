
name := "adapter-gsn"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.2"

libraryDependencies ++= Seq(
  "com.sun.jersey" % "jersey-client" % "1.8",
  "com.sun.jersey" % "jersey-core" % "1.8",
  "com.google.code.gson" % "gson" % "1.7.1",
  "es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.4",
//  "es.upm.fi.oeg.morph.streams" % "esper-engine" % "1.0.1",
  "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test",
  "org.scalacheck" % "scalacheck_2.10" % "1.10.0" % "test",
  "org.jibx" % "jibx-bind" % "1.2.2",
  "commons-collections" % "commons-collections" % "3.2.1",
  "ch.epfl.lsir" % "gsn" % "1.1.2"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",  
  "aldebaran-libs" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-external-libs-local"  
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")
