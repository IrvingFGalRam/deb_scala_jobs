ThisBuild / scalaVersion     := "2.12.14"
ThisBuild / version          := "0.1.1"

lazy val root = (project in file("."))
  .settings(
    name := "scala-data-proc",
    scalaVersion := "2.12.14",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.3" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.1.3"% "provided",
      "org.apache.spark" %% "spark-avro" % "3.1.3" % "provided",
      "com.databricks" %% "spark-xml" % "0.13.0" % "provided"
//      scalaTest % Test
    )
  )