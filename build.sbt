ThisBuild / scalaVersion     := "2.13.0"
ThisBuild / version          := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "scala-data-proc",
    scalaVersion := "2.13.0",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
      "org.apache.spark" %% "spark-sql" % "3.2.1",
      "org.apache.spark" %% "spark-mllib" % "3.2.1",
      "org.apache.spark" %% "spark-avro" % "3.2.1" % "compile",
      "com.databricks" %% "spark-xml" % "0.15.0" % "compile"
//      "org.apache.spark" %% "spark-sql" % "3.2.1",
//      scalaTest % Test
    )
  )