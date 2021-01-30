name := "boston_crimes_stat_grigoriev_alexey"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided,
  "com.typesafe" % "config" % "1.4.1"
)
