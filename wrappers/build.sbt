name := "wrappers"

organization := "es.upm.fi.oeg.morph.streams"

version := "1.0.10"

scalaVersion := "2.11.2"

crossPaths := false

libraryDependencies ++= Seq(
  "es.upm.fi.oeg.morph.streams" % "esper-engine" % "1.0.4",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "com.typesafe.play" %% "play" % "2.3.4",
  "joda-time" % "joda-time" % "2.1",    
  "org.joda" % "joda-convert" % "1.2",  
  "org.codehaus.jackson" % "jackson-core-asl" % "1.9.10",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.10",
  "com.typesafe.akka" %% "akka-actor" % "2.3.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "log4j" % "log4j" % "1.2.17",
  "org.jibx" % "jibx-bind" % "1.2.2",
  "commons-collections" % "commons-collections" % "3.2.1",  
  "ch.epfl.lsir" % "gsn" % "1.1.2"  
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
  "aldebaran-external" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-external-libs-local",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "sonatype-public" at "https://oss.sonatype.org/​content/repositories/public"  
)

//seq(scalaxbSettings: _*)

//packageName in scalaxb in Compile := "es.emt.wsdl"

//sourceGenerators in Compile <+= scalaxb in Compile

scalacOptions += "-deprecation"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_))

unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))

parallelExecution in Test := false

publishTo := Some("Artifactory Realm" at "http://planetdata.epfl.ch:8081/artifactory/gsn-release")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishMavenStyle := true

publishArtifact in (Compile, packageSrc) := false
