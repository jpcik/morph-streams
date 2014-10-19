
name := "rdf-streams"

organization := "org.rsp"

version := "0.0.1"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.11",
  "eu.trowl" % "trowl-core" % "1.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",    
  "aldebaran-libs" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-external-libs-local",
  "gsn" at "http://planetdata.epfl.ch:8081/artifactory/gsn-release"
)
