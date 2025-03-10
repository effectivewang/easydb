load("@rules_java//java:defs.bzl", "java_library", "java_test", "java_binary")
load("@rules_jvm_external//:extensions.bzl", "maven")

package(default_visibility = ["//visibility:public"])

JAVA_OPTS = [
    "--enable-preview",
    "--release=21",
    "-source", "21",
    "-target", "21",
    "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED"
]

java_library(
    name = "core",
    srcs = glob(["src/main/java/com/easydb/core/**/*.java"]),
    javacopts = JAVA_OPTS,
)

java_library(
    name = "index",
    srcs = glob(["src/main/java/com/easydb/index/**/*.java"]),
    deps = [
        ":core",
    ],
    javacopts = JAVA_OPTS,
)

java_library(
    name = "storage",
    srcs = glob(["src/main/java/com/easydb/storage/**/*.java", 
                "src/main/java/com/easydb/storage/metadata/**/*.java",
                "src/main/java/com/easydb/storage/transaction/**/*.java"]),
    deps = [
        ":core",
        ":index",
    ],
    javacopts = JAVA_OPTS,
)

java_binary(
    name = "easydb",
    srcs = glob(["src/main/java/com/easydb/*.java"]),
    main_class = "com.easydb.Main",
    deps = [
        ":storage",
        ":sql",
        ":core",
    ],
    javacopts = JAVA_OPTS,
)

java_library(
    name = "sql",
    srcs = glob(["src/main/java/com/easydb/sql/**/*.java"]),
    deps = [
        ":core",
        ":storage",
        ":index",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
    ],
    javacopts = JAVA_OPTS,
)

java_test(
    name = "sql-test",
    srcs = glob(["src/test/java/com/easydb/sql/**/*.java"]),
    use_testrunner = False,
    main_class = "org.junit.platform.console.ConsoleLauncher",
    args = [
        "--select-package=com.easydb.sql",
        "--details=verbose", 
    ],
    deps = [
        ":core",
        ":storage",
        ":sql",
        ":index",
        "@maven//:org_junit_jupiter_junit_jupiter_api",
        "@maven//:org_junit_jupiter_junit_jupiter_engine",
        "@maven//:org_junit_platform_junit_platform_console",
        "@maven//:org_junit_platform_junit_platform_launcher",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_junit_jupiter",
    ],
    javacopts = JAVA_OPTS,
) 

java_test(
    name = "debug",
    srcs = ["src/test/java/com/easydb/sql/MVCCSqlTest.java"],
    use_testrunner = True,
    test_class = "com.easydb.sql.MVCCSqlTest",
    main_class = "org.junit.platform.console.ConsoleLauncher",
    args = [
        "--select-package=com.easydb.sql",
        "--details=verbose", 

    ],
    deps = [
        ":core",
        ":storage",
        ":sql",
        ":index",
        "@maven//:org_junit_jupiter_junit_jupiter_api",
        "@maven//:org_junit_jupiter_junit_jupiter_engine",
        "@maven//:org_junit_platform_junit_platform_console",
        "@maven//:org_junit_platform_junit_platform_launcher",
        "@maven//:com_fasterxml_jackson_core_jackson_databind",
        "@maven//:com_fasterxml_jackson_core_jackson_core",
        "@maven//:com_fasterxml_jackson_core_jackson_annotations",
        "@maven//:org_mockito_mockito_core",
        "@maven//:org_mockito_mockito_junit_jupiter",
    ],
    javacopts = JAVA_OPTS,
) 