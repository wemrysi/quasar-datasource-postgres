import scala.collection.Seq

performMavenCentralSync in ThisBuild := false   // basically just ignores all the sonatype sync parts of things

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-datasource-postgres"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-datasource-postgres"),
  "scm:git@github.com:slamdata/quasar-datasource-postgres.git"))

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-datasource-postgres",

    quasarPluginName := "postgres",

    quasarPluginQuasarVersion := IO.read(file("./quasar-version")).trim,

    quasarPluginDatasourceFqcn := Some("quasar.plugin.postgres.PostgresDatasourceModule$"),

    /** Specify managed dependencies here instead of with `libraryDependencies`.
      * Do not include quasar libs, they will be included based on the value of
      * `quasarPluginQuasarVersion`.
      */
    quasarPluginDependencies ++= Seq(
    ))
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
