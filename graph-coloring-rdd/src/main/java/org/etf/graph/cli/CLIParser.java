package org.etf.graph.cli;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.etf.graph.config.GraphConfiguration;

/**
 * Centralizovana klasa za parsiranje i validaciju svih argumenata komandne linije (CLI).
 * Odgovoran je za pretvaranje CLI opcija u validan konfiguracioni objekat.
 */
public class CLIParser {

    private static final Logger LOG = LogManager.getLogger(CLIParser.class);

    public static GraphConfiguration parse(String[] args) throws ParseException, IllegalArgumentException {

        Options options = createOptions();
        CommandLine cmd = parseArguments(options, args);

        if (cmd == null) {
            throw new IllegalArgumentException("Došlo je do greške prilikom parsiranja CLI argumenata.");
        }

        if (cmd.hasOption("help")) {
            printHelp(options);
            throw new HelpRequestedException();
        }

        validateMandatoryOptions(cmd);

        String inputPath = cmd.getOptionValue("input");
        Integer numNodes = parseIntegerOption(cmd, "generate");
        Integer maxDegree = parseIntegerOption(cmd, "max-degree");
        Long seed = parseLongSeedOption(cmd);

        Integer initialColors = parseIntegerOption(cmd, "initial-colors");
        Integer numPartitions = parseIntegerOption(cmd, "partitions");

        boolean runAverageCheck = cmd.hasOption("run-average");
        boolean runColoring = cmd.hasOption("run-coloring");
        boolean runValidation = cmd.hasOption("run-validation");
        boolean runIncremental = cmd.hasOption("run-incremental");

        boolean measureTime = cmd.hasOption("measure-time");
        String metricsOutPath = cmd.getOptionValue("metrics-out");

        String outputGraphPath = cmd.getOptionValue("output-graph");
        String outputResultPath = cmd.getOptionValue("output-result");

        boolean useBaseline = cmd.hasOption("use-baseline");

        String changesFilePath = cmd.getOptionValue("changes-file");

        return new GraphConfiguration(inputPath, numNodes, maxDegree, seed,
                initialColors, numPartitions,
                runAverageCheck, runColoring,
                runValidation, runIncremental,
                measureTime, metricsOutPath,
                outputGraphPath, outputResultPath,
                useBaseline, changesFilePath );
    }

    private static Options createOptions() {
        Options options = new Options();

        // 1. Opšte opcije za ulaz/generisanje
        options.addOption("i", "input", true, "Putanja do JSON fajla sa definicijom grafa");
        options.addOption("g", "generate", true, "Ukupan broj čvorova za generisanje grafa");
        options.addOption("m", "max-degree", true, "Maksimalni dozvoljeni stepen čvora pri generisanju");
        options.addOption("s", "seed", true, "Seed za generisanje pseudo-slučajnog grafa");
        // 2. Konfiguracija bojenja
        options.addOption(null, "partitions", true, "Eksplicitno podešavanje broja particija za Spark RDD");
        options.addOption(null, "initial-colors", true, "Početni broj boja (K) za početak iterativnog bojenja");
        // 3. Opcije za izvršavanje poslova (Jobovi)
        options.addOption(null, "run-average", false, "Omogući Spark job za proračun prosečnog stepena grafa");
        options.addOption(null, "run-coloring", false, "Omogući Spark job za distribuirano bojenje grafa");
        options.addOption(null, "run-validation", false, "Omogući Spark job za provjeru ispravnosti bojenja (validacija)");
        options.addOption(null, "run-incremental", false, "Omogući Spark job za inkrementalno (postepeno) bojenje");
        // 4. Opcije za metrike/vrijeme
        options.addOption(null, "measure-time", false, "Omogući mjerenje i praćenje vremena izvršavanja svih poslova");
        options.addOption(null, "metrics-out", true, "Putanja do CSV fajla za zapis metrika izvršavanja");
        // 5. Opcije za izlaz
        options.addOption(null, "output-graph", true, "Izlazni fajl za serijalizaciju stanja grafa (JSON)");
        options.addOption(null, "output-result", true, "Izlazni fajl za serijalizaciju rezultata bojenja");
        // 6. Baseline verzija algoritma za bojenje grafa radi prikaza efekata optimizacije
        options.addOption(null, "use-baseline", false, "Koristi neoptimizovanu verziju algoritma za poređenje performansi");
        // 7. Opcija za promjene
        options.addOption(null, "changes-file", true, "Putanja do fajla sa definicijom promjena za inkrementalno bojenje");
        // 8. Pomoć
        options.addOption("h", "help", false, "Prikaz pomoći");

        return options;
    }

    private static CommandLine parseArguments(Options options, String[] args) {
        try {
            return new DefaultParser().parse(options, args);
        } catch (Exception e) {
            LOG.error("Greška prilikom parsiranja: {}", e.getMessage());
            printHelp(options);
            return null;
        }
    }

    private static void validateMandatoryOptions(CommandLine cmd) throws IllegalArgumentException {

        boolean hasInput = cmd.hasOption("input");
        boolean hasGenerate = cmd.hasOption("generate") && cmd.hasOption("max-degree");
        if (!hasInput && !hasGenerate) {
            throw new IllegalArgumentException(
                    """
                                Graf mora biti definisan. Potrebno je:\s
                                1. Koristiti --input <fajl> ili
                                2. Koristiti --generate <čvorovi> --max-degree <stepen>.
                            """
            );
        }

        if (!cmd.hasOption("run-average") && !cmd.hasOption("run-coloring")
                && !cmd.hasOption("run-validation") && !cmd.hasOption("run-incremental")) {
            throw new IllegalArgumentException(
                    """
                                Nije definisan nijedan Spark Job za izvršavanje. Potrebno je odabrati bar jednu opciju:
                                - --run-average
                                - --run-coloring
                                - --run-validation
                                - --run-incremental
                            """
            );
        }

        boolean isColoringJob = cmd.hasOption("run-coloring") || cmd.hasOption("run-incremental");
        if (cmd.hasOption("initial-colors") && !isColoringJob) {
            throw new IllegalArgumentException(
                    """
                                Greška u konfiguraciji: Navedena je opcija --initial-colors,
                                ali nije aktiviran nijedan Spark Job za bojenje grafa (--run-coloring ili --run-incremental).
                            """
            );
        }

        if (cmd.hasOption("run-incremental") && !cmd.hasOption("run-coloring")) {
            throw new IllegalArgumentException(
                    """
                                Greška u konfiguraciji: Inkrementalno bojenje (--run-incremental)
                                zahteva prethodno izvršavanje inicijalnog bojenja grafa (--run-coloring)
                                kako bi se odredio optimalni broj boja (K).
                            """
            );
        }

        if (cmd.hasOption("metrics-out") && !cmd.hasOption("measure-time")) {
            throw new IllegalArgumentException(
                    """
                                Greška u konfiguraciji: Zahtijevana je izlazna putanja za metrike, ali mjerenje vremena nije aktivirano.
                                Opcija --metrics-out zahteva obavezno korišćenje opcije --measure-time\s
                                kako bi se omogućilo prikupljanje podataka pre njihovog zapisivanja.
                            """
            );
        }

        if (cmd.hasOption("output-graph")) {
            boolean canSaveGraph = hasGenerate
                    || cmd.hasOption("run-coloring")
                    || cmd.hasOption("run-incremental");

            if (!canSaveGraph) {
                throw new IllegalArgumentException(
                        """
                                    Nevažeća upotreba opcije --output-graph.
                                    Graf se može snimiti samo ako se dešava neka izmjena:
                                    • Generisanje novog grafa (--generate), ili
                                    • Bojenje grafa (--run-coloring), ili
                                    • Inkrementalne promjene (--run-incremental).
                                """
                );
            }
        }

        if (cmd.hasOption("output-result")) {
            boolean canSaveResult = cmd.hasOption("run-coloring")
                    || cmd.hasOption("run-incremental");

            if (!canSaveResult) {
                throw new IllegalArgumentException(
                        """
                                    Greška u konfiguraciji: Opcija --output-result zahteva bojenje grafa.
                                    Potrebno je koristiti jednu od sledećih opcija:
                                      • --run-coloring (inicijalno bojenje), ili
                                      • --run-incremental (inkrementalno bojenje poslije promjena).
                                """
                );
            }
        }

        if (cmd.hasOption("input") && cmd.hasOption("output-graph")) {
            String inputPath = cmd.getOptionValue("input");
            String outputPath = cmd.getOptionValue("output-graph");

            try {
                java.nio.file.Path inputNorm = java.nio.file.Paths.get(inputPath).toAbsolutePath().normalize();
                java.nio.file.Path outputNorm = java.nio.file.Paths.get(outputPath).toAbsolutePath().normalize();

                if (inputNorm.equals(outputNorm)) {
                    throw new IllegalArgumentException(
                            """
                                        Greška: Izlazna putanja --output-graph ne može biti ista kao ulazna --input.
                                        Input:  %s
                                        Output: %s
                                        Ovo bi prepisalo originalni fajl!
                                    """.formatted(inputPath, outputPath)
                    );
                }
            } catch (java.nio.file.InvalidPathException e) {
                LOG.warn("Nije mmoguće validirati putanje: {}", e.getMessage());
            }
        }

        if (cmd.hasOption("output-graph")
                && hasGenerate
                && !cmd.hasOption("run-coloring")
                && !cmd.hasOption("run-incremental")) {

            LOG.warn(
                    """
                                Upozorenje: Graf će biti snimljen bez bojenja!
                                Da bi se obojio graf prije snimanja treba koristiti opciju: --run-coloring
                            """
            );
        }

        if (cmd.hasOption("use-baseline") && !cmd.hasOption("run-coloring")) {
            throw new IllegalArgumentException(
                    """
                                Greška u konfiguraciji: Opcija --use-baseline može se koristiti samo uz --run-coloring.
                                Baseline verzija algoritma se koristi za poređenje performansi sa optimizovanom verzijom.
                            """
            );
        }

        if (cmd.hasOption("use-baseline") && cmd.hasOption("run-incremental")) {
            throw new IllegalArgumentException(
                    """
                                Greška u konfiguraciji: --use-baseline se ne može koristiti sa inkrementalnim bojenjem (--run-incremental).
                                Baseline verzija se koristi isključivo za poređenje inicijalnog bojenja.
                            """
            );
        }

    }

    private static Integer parseIntegerOption(CommandLine cmd, String opt) {
        if (!cmd.hasOption(opt))
            return null;
        int value;
        try {
            value = Integer.parseInt(cmd.getOptionValue(opt));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Vrijednost za opciju --" + opt + " mora biti cio broj.");
        }
        if (value <= 0) {
            throw new IllegalArgumentException("Vrijednost za opciju --" + opt + " mora biti pozitivan cio broj (veći od nule).");
        }
        return value;
    }

    private static Long parseLongSeedOption(CommandLine cmd) {
        if (!cmd.hasOption("seed"))
            return 123L;
        try {
            return Long.parseLong(cmd.getOptionValue("seed"));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Vrijednost za opciju --" + "seed" + " mora biti cio broj.");
        }
    }

    @SuppressWarnings("deprecation")
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.setDescPadding(5);
        formatter.setLeftPadding(1);
        String header = "\n------------------------------- Dostupne opcije -------------------------------\n\n";
        String footer = "\n------------------------------------------------------------------------------------";
        formatter.printHelp(
                " ",        // prazan string eliminiše "usage: MainApp"
                header,
                options,
                footer,
                false
        );
    }

}