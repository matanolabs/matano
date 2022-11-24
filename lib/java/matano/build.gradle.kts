import org.jetbrains.kotlin.incremental.mkdirsOrThrow
import java.nio.file.Paths

plugins {
    java
    kotlin("jvm") version "1.7.0" apply false
}

subprojects {
    repositories {
        mavenCentral()
        maven(url = "https://jitpack.io")
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
        inputs.files("${subproject.projectDir}/src")
//        outputs.files(subproject.buildDir)
        dependsOn("shadowJar")
        val assetName = if (subproject.name == "scripts") "matano-java-scripts" else subproject.name
        val assetDir = Paths.get(project.rootDir.absolutePath, "../../../local-assets/$assetName").toFile()
        val assetDirp = assetDir.canonicalPath
        Paths.get(assetDirp, "lib").toFile().mkdirsOrThrow()

        doLast {
            File("$assetDirp/placeholder-for-cdk.txt").createNewFile()
            exec {
                commandLine("bash", "-c", "cp -a ${subproject.buildDir}/libs/* $assetDirp/lib")
            }
        }
    }
}
