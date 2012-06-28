name := "adapter-gsn"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.0"

scalaVersion := "2.9.1"

crossPaths := false

libraryDependencies ++= Seq(
  "com.sun.jersey" % "jersey-client" % "1.8",
  "com.sun.jersey" % "jersey-core" % "1.8",
  "com.google.code.gson" % "gson" % "1.7.1",
  "es.upm.fi.oeg.morph" % "query-rewriting" % "1.0.0",
  "org.scalatest" % "scalatest_2.9.1" % "1.7.2" % "test",
  "org.scalacheck" % "scalacheck_2.9.1" % "1.9" % "test",
  "org.jibx" % "jibx-bind" % "1.2.2",
  "ch.epfl.lsir" % "gsn" % "1.1.2"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "aldebaran-snapshots" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-snapshots-local",
  "aldebaran-libs" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-external-libs-local"  
)

scalacOptions += "-deprecation"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))
