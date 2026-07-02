package org.etf.spotify.analysis.analyzer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.etf.spotify.analysis.exporter.ResultExporter;

import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Analizator kolaboracija između umjetnika.
 */
public class CollaborationAnalyzer implements Analyzer {

    @Override
    public void analyze(Dataset<Row> data) {

        /*
            Identifikacija Kolaboracija.
            Filtrira pjesme koje su kolaboracije između 2+ umjetnika.
            Kolona "artists" sadrži imena izvođača odvojena sa ";" (separator)
        */
        Dataset<Row> collaborations = data
                .filter(col("artists").contains(";"))
                .withColumn("num_artists", size(split(col("artists"), ";")))
                .filter(col("num_artists").gt(1));

        /*
            Računanje Statistike Top 10 Kolaboracija
            Grupišemo po kombinaciji umjetnika i računamo:
                collaboration_count - Koliko pјesama ima ta kolaboracija
                avg_collab_popularity - Prosečna popularnost zajedničkih pjesama
        */
        Dataset<Row> topCollaborations = collaborations
                .groupBy("artists")
                .agg(
                        count("*").alias("collaboration_count"),
                        avg("popularity").alias("avg_collab_popularity")
                )
                .orderBy(col("collaboration_count").desc())
                .limit(10);

        /*
            Računanje solo prosjek za sve umjetnike odjednom.
            Kolona se preimenuje za JOIN.
        */
        Dataset<Row> soloArtistsStats = data
                .filter(not(col("artists").contains(";")))
                .groupBy("artists")
                .agg(avg("popularity").alias("solo_avg_popularity"))
                .withColumnRenamed("artists", "solo_artist");

        /*
            Konvertuje se Spark Dataset u Java HashMap za brz pristup (optimizacija get() u odnosu na filter()).
            Map<String, Double> -> <umjetnik, prosjek>
            Proverava se artist != null && popularity != null, jer Spark može vratiti null ako nema solo pesama.
        */
        Map<String, Double> soloPopMap = new HashMap<>();
        for (Row row : soloArtistsStats.collectAsList()) {
            String artist = row.getAs("solo_artist");
            Double popularity = row.getAs("solo_avg_popularity");
            if (artist != null && popularity != null) {
                soloPopMap.put(artist, popularity);
            }
        }

        /*
            Za svaku od top 10 kolaboracija:
                1. Parsiramo umjetnike (split po ";")
                2. Nalazimo manje popularnog umjetnika (najniži solo prosek)
                3. Računamo "boost" = popularnost kolaboracije - solo popularnost
            Popularity boost > 0 -> Kolaboracija pomaže manje popularnom umjetniku
            Popularity boost < 0 -> Kolaboracija škodi manje popularnom umjetniku (ovo je neobično, ali moguće)
        */
        List<Map<String, Object>> collabList = new ArrayList<>();

        for (Row row : topCollaborations.collectAsList()) {
            String artists = row.getAs("artists");
            Long count = row.getAs("collaboration_count");
            Double avgPop = row.getAs("avg_collab_popularity");

            Map<String, Object> collabData = new LinkedHashMap<>();
            collabData.put("artists", artists);
            collabData.put("collaboration_count", count);
            collabData.put("avg_popularity", avgPop);

            if (avgPop != null) {
                // Nađi umjetnika sa najmanjom solo popularnosti
                String[] artistList = artists.split(";");
                String lessPopularArtist = "";
                double minPop = Double.MAX_VALUE;

                for (String artist : artistList) {
                    String trimmed = artist.trim();
                    Double soloPop = soloPopMap.get(trimmed);

                    if (soloPop != null && soloPop < minPop) {
                        minPop = soloPop;
                        lessPopularArtist = trimmed;
                    }
                }

                if (!lessPopularArtist.isEmpty()) {
                    collabData.put("less_popular_artist", lessPopularArtist);
                    collabData.put("solo_popularity", minPop);
                    collabData.put("popularity_boost", avgPop - minPop);
                }
            }

            collabList.add(collabData);
        }

        /*
            Računanje prosječnog Boost-a preko svih kolaboracija.
            Cilj je utvrditi da li kolaboraci u projseku povećavaju ili smanjuju popularnost.
        */
        double avgBoost = collabList.stream()
                .filter(c -> c.containsKey("popularity_boost"))
                .mapToDouble(c -> (Double) c.get("popularity_boost"))
                .average()
                .orElse(0.0);


        // Izvoz rezultata u JSON
        Map<String, Object> result = ResultExporter.createResult(
                "Top 10 Kolaboracija Između Umjetnika",
                "Identifikacija najčešćih kolaboracija i analiza njihove popularnosti"
        );
        result.put("total_collaborations", collaborations.count());
        result.put("top_10_collaborations", collabList);
        result.put("average_popularity_boost", avgBoost);
        result.put("conclusion", avgBoost > 0 ?
                "Kolaboracije u prosjeku povećavaju popularnost" :
                "Kolaboracije u prosjeku smanjuju popularnost");

        ResultExporter.exportAnalysis(
                "02_collaboration_analysis.json",
                result,
                "Top 10 Kolaboracija\n" +
                        "Analizirano kolaboracija: " + collaborations.count() + "\n" +
                        "Prosječan boost popularnosti: " + String.format("%.2f", avgBoost) + "\n" +
                        "Detalji: `02_collaboration_analysis.json`"
        );
    }

}