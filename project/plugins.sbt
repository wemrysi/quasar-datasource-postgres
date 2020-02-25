resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayIvyRepo("slamdata-inc", "sbt-plugins")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("com.slamdata" % "sbt-slamdata" % "5.4.0-95911d1")
addSbtPlugin("com.slamdata" % "sbt-quasar-plugin" % "0.2.4")
