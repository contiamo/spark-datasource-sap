enablePlugins(GitVersioning)
git.useGitDescribe := true

name := "spark-datasource-sap"
organization := "com.contiamo"

scalaVersion := "2.12.11"

fork in Test := true

val contiamoReleasesRepo = "contiamo-releases-repo" at "https://artifactory.contiamo.io/artifactory/contiamo-releases"
val contiamoSnapshotsRepo = "contiamo-snapshots-repo" at "https://artifactory.contiamo.io/artifactory/contiamo-snapshots"

publishTo := {
  if (isSnapshot.value) Some(contiamoSnapshotsRepo)
  else Some(contiamoReleasesRepo)
}

credentials += sys.env
  .get("ARTIFACTORY_PASSWORD")
  .map { password =>
    Credentials("Artifactory Realm",
                "artifactory.contiamo.io",
                sys.env.getOrElse("ARTIFACTORY_USERNAME", "ci"),
                password)
  }
  .getOrElse(Credentials(Path.userHome / ".ivy2" / ".credentials"))

libraryDependencies ++= {
  val sparkVersion = "3.0.1"
  Seq(
    "com.typesafe" % "config" % "1.4.0" % "test",
    "org.scalatest" %% "scalatest" % "3.1.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",
    "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % "test",
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided withSources(),
    "com.github.bigwheel" %% "util-backports" % "2.1"
  )
}
