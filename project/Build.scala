import sbt._
import Keys._

object HelloBuild extends Build {
  lazy val root = Project(id = "morph-streams",
                          base = file(".")) aggregate(sparqlstream, queryrewriting,esper,gsn)

  lazy val sparqlstream = Project(id = "sparql-stream",
                          base = file("sparql-stream"))

  lazy val queryrewriting = Project(id = "query-rewriting",
                              base = file("query-rewriting")) dependsOn(sparqlstream)

  lazy val esper = Project(id = "adapter-esper",
                              base = file("adapter-esper")) dependsOn(queryrewriting)

  lazy val gsn = Project(id = "adapter-gsn",
                              base = file("adapter-gsn")) dependsOn(queryrewriting)

//  lazy val r2rmlTc = Project(id = "morph-r2rml-tc",
//                             base = file("morph-r2rml-tc")) dependsOn(querygen)
}
