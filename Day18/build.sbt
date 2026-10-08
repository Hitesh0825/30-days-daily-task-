ThisBuild / scalaVersion := "2.12.18"
lazy val root = (project in file(".")).settings(
  name := "Day18",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.5.6",
    "org.apache.spark" %% "spark-sql" % "3.5.6"
  )
)
