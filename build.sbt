enablePlugins(GitVersioning)

git.useGitDescribe := true

name := "spark-datasource-sap"
organization := "com.contiamo"

scalaVersion := "2.12.11"

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
  val sparkVersion = "2.4.4"
  Seq(
    "org.scalatest" %% "scalatest" % "3.1.1" % "test",
    "com.typesafe" % "config" % "1.4.0" % "test",
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "com.github.bigwheel" %% "util-backports" % "2.1"
  )
}
