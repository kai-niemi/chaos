package io.roach.chaos;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.util.StringUtils;

import io.roach.chaos.model.IsolationLevel;
import io.roach.chaos.model.LockType;
import io.roach.chaos.model.WorkloadType;
import io.roach.chaos.util.AnsiColor;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.ConsoleOutput;
import io.roach.chaos.util.Multiplier;

public class Main {
    public static void main(String[] args) {
        Map<String, Object> properties = new LinkedHashMap<>();

        parseArgs(args, properties);

        new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.CONSOLE)
                .logStartupInfo(true)
                .properties(properties)
                .run(args);
    }

    private static void parseArgs(String[] args, Map<String, Object> properties) {
        LinkedList<String> argsList = new LinkedList<>(List.of(args));

        Set<String> springProfiles = new LinkedHashSet<>();
        String url = "";
        String user = "";
        String password = "";

        WorkloadType workloadType = null;

        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.startsWith("--")) {
                if (arg.equals("--verbose")) {
                    properties.put("chaos.debugProxy", true);
                    springProfiles.add("verbose");
                } else if (arg.equals("--jitter")) {
                    properties.put("chaos.retryJitter", true);
                } else if (arg.equals("--quit")) {
                    properties.put("chaos.quit", true);
                } else if (arg.equals("--export")) {
                    properties.put("chaos.export", true);
                } else if (arg.equals("--skip-create")) {
                    properties.put("chaos.skipCreate", true);
                } else if (arg.equals("--skip-init")) {
                    properties.put("chaos.skipInit", true);
                } else if (arg.equals("--skip-retry")) {
                    properties.put("chaos.skipRetry", true);
                } else if (arg.equals("--isolation")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }

                    try {
                        String level = argsList.pop();

                        properties.put("chaos.isolationLevel", EnumSet.allOf(IsolationLevel.class)
                                .stream()
                                .filter(i -> i.alias().equalsIgnoreCase(level) || i.name().equalsIgnoreCase(level))
                                .findFirst()
                                .orElseGet(() -> IsolationLevel.valueOf(level)));
                    } catch (IllegalArgumentException e) {
                        printUsageAndQuit("Bad name/alias: " + e.getLocalizedMessage());
                    }
                } else if (arg.equals("--locking")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    try {
                        String lockType = argsList.pop();

                        properties.put("chaos.lockType", EnumSet.allOf(LockType.class)
                                .stream()
                                .filter(i -> i.alias().equalsIgnoreCase(lockType) || i.name()
                                        .equalsIgnoreCase(lockType))
                                .findFirst()
                                .orElseGet(() -> LockType.valueOf(lockType)));
                    } catch (IllegalArgumentException e) {
                        printUsageAndQuit("Bad name/alias: " + e.getLocalizedMessage());
                    }
                } else if (arg.equals("--contention")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    int v = Integer.parseInt(argsList.pop());
                    if (v % 2 != 0 || v < 2) {
                        printUsageAndQuit("Contention level must be a multiple of 2 and >= 2");
                    }
                    properties.put("chaos.contentionLevel", v);
                } else if (arg.equals("--threads")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    int v = Integer.parseInt(argsList.pop());
                    if (v <= 0) {
                        printUsageAndQuit("Workers must be > 0");
                    }
                    properties.put("chaos.workers", v);
                } else if (arg.equals("--iterations")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    int v = Multiplier.parseInt(argsList.pop());
                    if (v <= 0) {
                        printUsageAndQuit("Iterations must be > 0");
                    }
                    properties.put("chaos.iterations", v);
                } else if (arg.equals("--accounts")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    int v = Multiplier.parseInt(argsList.pop());
                    if (v <= 0) {
                        printUsageAndQuit("Accounts must be > 0");
                    }
                    properties.put("chaos.numAccounts", v);
                } else if (arg.equals("--selection")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    int v = Multiplier.parseInt(argsList.pop());
                    if (v <= 0) {
                        printUsageAndQuit("Selection must be > 0");
                    }
                    properties.put("chaos.selection", v);
                } else if (arg.equals("--url")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    url = argsList.pop();
                } else if (arg.equals("--user")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    user = argsList.pop();
                } else if (arg.equals("--password")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    password = argsList.pop();
                } else if (arg.equals("--profile")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    springProfiles.add(argsList.pop());
                } else if (arg.equals("--help")) {
                    printUsageAndQuit("");
                } else {
                    printUsageAndQuit("Unknown option: " + arg);
                }
            } else {
                try {
                    workloadType = EnumSet.allOf(WorkloadType.class)
                            .stream()
                            .filter(wt -> wt.alias().equalsIgnoreCase(arg) || wt.name().equalsIgnoreCase(arg))
                            .findFirst()
                            .orElseGet(() -> WorkloadType.valueOf(arg));
                    properties.put("chaos.workloadType", workloadType);
                } catch (IllegalArgumentException e) {
                    printUsageAndQuit("Unknown workload: " + arg);
                }
            }
        }

        if (!springProfiles.contains("crdb")
                && !springProfiles.contains("psql")
                && !springProfiles.contains("mysql")
                && !springProfiles.contains("oracle")) {
            springProfiles.add("crdb");
        }

        if (springProfiles.contains("crdb")) {
            url = url.equals("") ? "jdbc:postgresql://localhost:26257/chaos?sslmode=disable" : url;
            user = user.equals("") ? "root" : user;
            password = password.equals("") ? "root" : password;
        } else if (springProfiles.contains("psql")) {
            url = url.equals("") ? "jdbc:postgresql://localhost:5432/chaos?sslmode=disable" : url;
            user = user.equals("") ? "root" : user;
            password = password.equals("") ? "root" : password;
        } else if (springProfiles.contains("mysql")) {
            url = url.equals("") ? "jdbc:mysql://localhost:3306/chaos" : url;
            user = user.equals("") ? "root" : user;
            password = password.equals("") ? "" : password;
        } else if (springProfiles.contains("oracle")) {
            url = url.equals("") ? "jdbc:oracle:thin:@//localhost:1521/freepdb1" : url;
            user = user.equals("") ? "system" : user;
            password = password.equals("") ? "root" : password;
        }

        properties.put("spring.datasource.url", url);
        properties.put("spring.datasource.username", user);
        properties.put("spring.datasource.password", password);
        properties.put("spring.profiles.active", StringUtils.collectionToCommaDelimitedString(springProfiles));

        System.setProperty("spring.profiles.active", StringUtils.collectionToCommaDelimitedString(springProfiles));

        if (workloadType == null) {
            printUsageAndQuit("Missing workload type");
        }
    }

    private static void printUsageAndQuit(String note) {
        ConsoleOutput.print("Usage: java -jar chaos.jar [options] <workload>", AnsiColor.BOLD_BRIGHT_WHITE);
        ConsoleOutput.info("");
        ConsoleOutput.header("Common options:");
        ConsoleOutput.printLeft("--help", "this help");
        ConsoleOutput.printLeft("--verbose", "enable verbose SQL trace logging", "(false)");
        ConsoleOutput.printLeft("--export", "export results to chaos.csv file", "(false)");
        ConsoleOutput.printLeft("--quit", "test connection to database and quit", "(false)");
        ConsoleOutput.info("");

        ConsoleOutput.header("Connection options:");

        ConsoleOutput.printLeft("--profile <db>", "database profile (url and credentials)", "(crdb)");
        ConsoleOutput.printLeft("  crdb", "use CockroachDB via pgJDBC");
        ConsoleOutput.printLeft("  psql", "use PostgreSQL via pgJDBC");
        ConsoleOutput.printLeft("  mysql", "use MySQL via mysql-connector");
        ConsoleOutput.printLeft("  oracle", "use Oracle via ojdbc8");

        ConsoleOutput.printLeft("--url", "override datasource URL",
                "(jdbc:postgresql://localhost:26257/chaos?sslmode=disable)");
        ConsoleOutput.printLeft("--user", "override datasource user name", "(root)");
        ConsoleOutput.printLeft("--password", "override datasource password", "(<empty>)");
        ConsoleOutput.info("");

        ConsoleOutput.header("DDL/DML options:");
        ConsoleOutput.printLeft("--skip-create", "skip DDL/create script at startup", "(false)");
        ConsoleOutput.printLeft("--skip-init", "skip DML/init script at startup", "(false)");
        ConsoleOutput.printLeft("--skip-retry", "skip client-side retries", "(false)");
        ConsoleOutput.info("");

        ConsoleOutput.header("Workload options:");

        ConsoleOutput.printLeft("--isolation", "set isolation level", "(1SR)");

        EnumSet.allOf(IsolationLevel.class)
                .forEach(isolationLevel -> ConsoleOutput.printLeft("  " + isolationLevel.name(),
                        isolationLevel.alias()));

        ConsoleOutput.printLeft("--locking", "enable optimistic (cas) or pessimistic locking", "(NONE)");

        EnumSet.allOf(LockType.class)
                .forEach(lockType -> ConsoleOutput.printLeft("  " + lockType.name(), lockType.alias()));

        int workers = Runtime.getRuntime().availableProcessors() * 2;

        ConsoleOutput.printLeft("--threads <num>", "max number of threads", "(host vCPUs x 2 = " + workers + ")");
        ConsoleOutput.printLeft("--iterations <num>", "number of cycles to run", "(1K)");
        ConsoleOutput.printLeft("--accounts <num>", "number of accounts to create and randomize between", "(50K)");
        ConsoleOutput.printLeft("--contention <num>", "contention level for the P4 lost update workload only", "(2)");
        ConsoleOutput.printLeft("--selection <num>", "random selection of accounts to pick between", "(500)");
        ConsoleOutput.info("  Hint: decrease selection to observe anomalies in read-committed.");
        ConsoleOutput.printLeft("--jitter", "enable exponential backoff jitter", "(false)");
        ConsoleOutput.info("  Hint: skip jitter for more comparable results between isolation levels.");
        ConsoleOutput.info("");

        ConsoleOutput.header("Workload types:");

        EnumSet.allOf(WorkloadType.class)
                .forEach(workloadType -> ConsoleOutput.printLeft("  " + workloadType.name(), workloadType.alias()));

        ConsoleOutput.info("");
        ConsoleOutput.error(note);
        ConsoleOutput.error(AsciiArt.shrug());

        System.exit(1);
    }
}
