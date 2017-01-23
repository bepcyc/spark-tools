import sbt.Keys.scalaVersion

name := "spark-tools"

version := "0.1.2"

scalaVersion := "2.10.6"

lazy val oldScalaVersion = "2.10.6"
lazy val newScalaVersion = "2.11.7"
lazy val newerScalaVersion = "2.11.7"

crossScalaVersions := Seq(oldScalaVersion, newScalaVersion, newerScalaVersion)

val sparkVersion = settingKey[String]("The version of Spark")

sparkVersion := sys.props.getOrElse("spark.version", getSparkVersion(scalaVersion.value))

def getSparkVersion(scalaVersion: String) = scalaVersion match {
  case `oldScalaVersion` => "1.6.3"
  case _ => "2.0.0"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided",
  "org.scalactic" %% "scalactic" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
