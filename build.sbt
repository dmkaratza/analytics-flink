name := "Hourly Analytics"

version := "0.1"

scalaVersion := "2.12.10"

resolvers ++= Seq(
  Resolver.typesafeRepo("releases")
)

lazy val flinkVersion     = "1.13.0"
lazy val catsVersion      = "2.4.2"
lazy val circeVersion     = "0.13.0"
lazy val chimneyVersion   = "0.6.1"
lazy val scalatestVersion = "3.2.6"

libraryDependencies ++= {
  Seq(
    "org.apache.flink" %% "flink-clients"         % flinkVersion,
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "io.circe"         %% "circe-core"            % circeVersion,
    "io.circe"         %% "circe-generic"         % circeVersion,
    "io.circe"         %% "circe-generic-extras"  % circeVersion,
    "io.circe"         %% "circe-parser"          % circeVersion,
    "io.scalaland"     %% "chimney"               % chimneyVersion,
    "org.apache.flink" %% "flink-test-utils"      % flinkVersion % Test,
    "org.apache.flink" %% "flink-runtime"         % flinkVersion % Test,
    "org.scalatest"    %% "scalatest"             % scalatestVersion % Test
  )
}
