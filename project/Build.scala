import sbt._
import Keys._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._

object HelloBuild extends Build {
  val scalaOnly = Seq (
    unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)(Seq(_)),
    unmanagedSourceDirectories in Test <<= (scalaSource in Test)(Seq(_))
  )
  val projSettings = Seq (
    scalaVersion := "2.11.2",
    crossPaths := false,
    scalacOptions += "-deprecation",
    parallelExecution in Test := false,
    resolvers ++= Seq(
      DefaultMavenRepository,
      "Local ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/local")
  )
  val ideSettings = Seq (
    EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
  )
  val publishSettings = Seq (
    publishTo := Some("Artifactory Realm" at "http://osper.epfl.ch:8081/artifactory/gsn-release"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    publishMavenStyle := true,
    publishArtifact in (Compile, packageSrc) := false )

  val buildSettings = Defaults.defaultSettings ++ projSettings ++ ideSettings ++ publishSettings 

  lazy val root = Project(id = "morph-streams",
                          base = file("."),settings = buildSettings) aggregate(sparqlstream, queryrewriting,gsn,esper)

  lazy val sparqlstream = Project(id = "sparql-stream",
                          base = file("sparql-stream"),settings = buildSettings)

  lazy val queryrewriting = Project(id = "query-rewriting",
                              base = file("query-rewriting"),settings = buildSettings ++ scalaOnly) dependsOn(sparqlstream)

  lazy val gsn = Project(id = "adapter-gsn",
                              base = file("adapter-gsn"),settings = buildSettings ++ scalaOnly) dependsOn(queryrewriting)

  lazy val esper = Project(id = "adapter-esper",
                              base = file("adapter-esper"),settings = buildSettings ++ scalaOnly) dependsOn(queryrewriting)

							  }
