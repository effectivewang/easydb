load("@rules_java//java:defs.bzl", "java_library", "java_test", "java_binary")
load("@rules_jvm_external//:extensions.bzl", "maven")

package(default_visibility = ["//visibility:public"])

# Java compilation settings for Java 21
JAVA_OPTS = [
    "--enable-preview",
    "--release=21",
    "-source", "21",
    "-target", "21",
]

# Core library
java_library(
    name = "easydb-core",
    srcs = glob(["src/main/java/com/easydb/core/*.java"]),
    visibility = ["//visibility:public"],
    exports = ["@maven//:com_google_guava_guava"],
    javacopts = JAVA_OPTS,
)

# SQL library
java_library(
    name = "easydb-sql",
    srcs = glob(["src/main/java/com/easydb/sql/**/*.java"]),
    deps = [":easydb-core"],
    visibility = ["//visibility:public"],
    javacopts = JAVA_OPTS,
)

# Tests
java_test(
    name = "result-serialization-test",
    srcs = ["src/test/java/com/easydb/sql/result/ResultSetSerializationTest.java"],
    test_class = "com.easydb.sql.result.ResultSetSerializationTest",
    deps = [
        ":easydb-core",
        ":easydb-sql",
        "@maven//:org_junit_jupiter_junit_jupiter_api",
        "@maven//:org_junit_jupiter_junit_jupiter_engine",
        "@maven//:org_junit_platform_junit_platform_launcher",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
    ],
    javacopts = JAVA_OPTS,
) 