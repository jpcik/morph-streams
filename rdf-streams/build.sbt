
name := "rdf-streams"

organization := "org.rsp"

version := "0.0.1"

scalaVersion := "2.11.2"

lazy val ldp = (project in file("webapp")).enablePlugins(PlayScala).dependsOn(rsp)

lazy val rsp = (project in file("."))

lazy val root = project.
  aggregate(rsp,ldp).
  settings(
    aggregate in update := false
  )
  
libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.11",
  "eu.trowl" % "trowl-core" % "1.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.jena" % "apache-jena-libs" % "2.12.1"
)

resolvers ++= Seq(
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",    
  "aldebaran-libs" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-external-libs-local",
  "gsn" at "http://planetdata.epfl.ch:8081/artifactory/gsn-release"
)
