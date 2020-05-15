name := "spark-datasource-sap"
organization := "com.contiamo"
version := "0.1-SNAPSHOT"

scalaVersion := "2.12.11"

val contiamoReleasesRepo = "contiamo-releases-repo" at "https://artifactory.contiamo.io/artifactory/contiamo-releases"
val contiamoSnapshotsRepo = "contiamo-snapshots-repo" at "https://artifactory.contiamo.io/artifactory/contiamo-snapshots"

publishTo := {
  if (isSnapshot.value) Some(contiamoSnapshotsRepo)
  else Some(contiamoReleasesRepo)
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

libraryDependencies ++= {
  val sparkVersion = "2.4.4"
  Seq(
    "org.scalatest" %% "scalatest" % "3.1.1" % "test",
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "com.github.bigwheel" %% "util-backports" % "2.1"
  )
}