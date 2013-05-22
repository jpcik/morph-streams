name := "morph-streams"

organization := "es.upm.fi.oeg.morph"

version := "1.0.6"

scalaVersion := "2.10.1"

crossPaths := false

libraryDependencies ++= Seq(
)

scalacOptions += "-deprecation"

EclipseKeys.skipParents in ThisBuild := false

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))

publish := {}

