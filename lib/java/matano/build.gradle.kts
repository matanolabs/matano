plugins {
    java
    kotlin("jvm") version "1.7.0" apply false
}

subprojects {
    repositories {
        mavenCentral()
    }
    val subproject = this
    tasks {
        withType<AbstractArchiveTask>().configureEach {
            isPreserveFileTimestamps = false
            isReproducibleFileOrder = true
        }
    }
    tasks.whenTaskAdded { // https://github.com/johnrengelman/shadow/issues/153
        if (listOf("shadowDistZip", "shadowDistTar", "distZip", "distTar").contains(this.name)) {
            this.enabled = false
        }
    }
    tasks.register("release") {
        dependsOn("shadowJar")
        if (File("/asset-output").exists()) {
            File("/asset-output/placeholder-for-cdk.txt").createNewFile() // Needed for CDK to prevent odd bugs.
        }
    }
}
