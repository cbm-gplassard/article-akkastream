name := "article-akkastream"

organization := "fr.glc"

version := "0.1"

scalaVersion := "2.13.3"

scalacOptions ++= Seq(
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
)

resolvers ++= Seq(
  "jsonlib-repo" at "https://raw.githubusercontent.com/mathieuancelin/json-lib-javaslang/master/repository/releases",
  Resolver.jcenterRepo
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % Versions.akka,
  "com.typesafe.akka" %% "akka-slf4j" % Versions.akka,
  "com.lightbend.akka" %% "akka-stream-alpakka-dynamodb" % Versions.alpakkaDynamo,
  "fr.maif" %% "izanami-client" % Versions.izanami,
  "software.amazon.awssdk" % "sts" % Versions.awsSdk % Runtime,
  "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka % Test,
  "org.scalatest" %% "scalatest" % Versions.scalaTest % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

mainClass := Some("fr.glc.guardians.scripts.fixups3.App")
