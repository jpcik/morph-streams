import com.typesafe.sbt.packager.Keys._

name := "ldp"

organization := "gsn"

version := "0.0.3-SNAPSHOT"

scalaVersion := "2.11.2"

crossPaths := false

scriptClasspath := Seq("*")

libraryDependencies ++= Seq(
  jdbc,
  ws,
  cache,
  "org.scalatestplus" %% "play" % "1.1.0" % "test"
  )

publishTo := Some("Artifactory Realm" at "http://planetdata.epfl.ch:8081/artifactory/gsn-release")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishMavenStyle := true

publishArtifact in (Test) := false

publishArtifact in (Compile) := false

publishArtifact in (Compile, packageBin) := true