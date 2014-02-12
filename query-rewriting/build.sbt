name := "query-rewriting"

organization := "es.upm.fi.oeg.morph"

version := "1.0.10"

libraryDependencies ++= Seq(
  "net.sf" % "jsqlparser" % "0.0.1" intransitive,
  "ch.qos.logback" % "logback-classic" % "1.0.13" intransitive,  
  "es.upm.fi.oeg.morph" % "morph-querygen" % "1.0.6" ,
  "com.typesafe.akka" %% "akka-actor" % "2.2.0" intransitive,
  "es.upm.fi.oeg.morph" % "kyrie" % "0.18.2",    
  "org.scalatest" % "scalatest_2.10" % "2.0.RC1" % "test"
)

resolvers ++= Seq(
  DefaultMavenRepository,
  "aldebaran-releases" at "http://aldebaran.dia.fi.upm.es/artifactory/sstreams-releases-local",
    "plord" at "http://homepages.cs.ncl.ac.uk/phillip.lord/maven",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.6")

