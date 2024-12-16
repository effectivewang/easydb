load("@rules_java//java:defs.bzl", "java_library", "java_test", "java_binary")
load("@rules_jvm_external//:extensions.bzl", "maven")

package(default_visibility = ["//visibility:public"])

# Java compilation settings for Java 21
JAVA_OPTS = [
    "--enable-preview",
    "--release=21",
]

# Core library
java_library(
    name = "easydb-core",
    srcs = glob(["src/main/java/com/easydb/core/**/*.java"]),
    deps = [
    ],
    javacopts = JAVA_OPTS,
)

# SQL Result library
java_library(
    name = "easydb-sql-result",
    srcs = glob(["src/main/java/com/easydb/sql/result/**/*.java"]),
    deps = [
        ":easydb-core",
    ],
    javacopts = JAVA_OPTS,
)

# Storage module
java_library(
    name = "easydb-storage",
    srcs = glob(["src/main/java/com/easydb/storage/**/*.java"]),
    deps = [
        ":easydb-core",
    ],
    javacopts = JAVA_OPTS,
)

# Index module
java_library(
    name = "easydb-index",
    srcs = glob(["src/main/java/com/easydb/index/**/*.java"]),
    deps = [
        ":easydb-core",
        ":easydb-storage",
    ],
    javacopts = JAVA_OPTS,
)

# SQL module
java_library(
    name = "easydb-sql",
    srcs = glob(["src/main/java/com/easydb/sql/**/*.java"]),
    deps = [
        ":easydb-core",
        ":easydb-storage",
        ":easydb-index",
    ],
    javacopts = JAVA_OPTS,
)

# Planner module
java_library(
    name = "easydb-planner",
    srcs = glob(["src/main/java/com/easydb/planner/**/*.java"]),
    deps = [
        ":easydb-core",
        ":easydb-storage",
        ":easydb-index",
        ":easydb-sql",
    ],
    javacopts = JAVA_OPTS,
)

# Main application
java_binary(
    name = "easydb",
    main_class = "com.easydb.Main",
    runtime_deps = [
        ":easydb-core",
        ":easydb-storage",
        ":easydb-index",
        ":easydb-sql",
        ":easydb-planner",
    ],
    javacopts = JAVA_OPTS,
)

# Tests
java_test(
    name = "result-serialization-test",
    srcs = ["src/test/java/com/easydb/sql/result/ResultSetSerializationTest.java"],
    deps = [
        ":easydb-core",
        ":easydb-sql-result",
        "@maven//:org_junit_jupiter_junit_jupiter_api",
        "@maven//:org_junit_jupiter_junit_jupiter_engine",
        "@maven//:org_junit_platform_junit_platform_launcher",
        "@maven//:org_junit_platform_junit_platform_console", 

    ],
    javacopts = JAVA_OPTS,
    use_testrunner = False,
    main_class = "org.junit.platform.console.ConsoleLauncher",
    args = [
        "--select-class=com.easydb.sql.result.ResultSetSerializationTest",
        "--details=tree",
    ],
) 