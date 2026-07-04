<br>
<br>
<div align="center">
  <h1>Spark: Distribuirano Bojenje Grafa i Analiza Spotify Skupa Podataka</h1> 
</div>
<br>

<div style="page-break-before: always;"></div>

Dva nezavisna Java/Spark projekta izrađena u okviru predmeta *Odabrana poglavlja iz operativnih sistema*: distribuirani algoritam za bojenje grafa zasnovan na **Spark RDD API-ju** (sa optimizovanom i baseline verzijom, te inkrementalnim bojenjem) i analiza Spotify skupa podataka zasnovana na **Spark DataFrame API-ju** (devet nezavisnih analiza nad ~114.000 pjesama). Oba projekta se pokreću lokalno (`local[*]`), bez potrebe za pravim Spark klasterom.

## Sadržaj

- [Struktura repozitorijuma](#struktura-repozitorijuma)
- [Modul 1: Distribuirano bojenje grafa](#modul-1-distribuirano-bojenje-grafa)
  - [Algoritam](#algoritam)
  - [CLI opcije](#cli-opcije)
  - [Optimizacije](#optimizacije)
  - [Inkrementalno bojenje](#inkrementalno-bojenje)
  - [Rezultati benchmark testiranja](#rezultati-benchmark-testiranja)
- [Modul 2: Analiza Spotify skupa podataka](#modul-2-analiza-spotify-skupa-podataka)
  - [Šema podataka i čišćenje](#šema-podataka-i-čišćenje)
  - [Analize](#analize)
  - [Rezime izvršavanja](#rezime-izvršavanja)
- [Tehnologije i alati](#tehnologije-i-alati)
- [Kako pokrenuti projekat lokalno](#kako-pokrenuti-projekat-lokalno)

## Struktura repozitorijuma

```
├── docs/
│   ├── Projektni_zadatak.pdf               # Specifikacija zadatka
│   ├── graph-coloring-optimization.pdf     # Opis optimizacija i rezultati mjerenja
│   ├── graph-coloring-cli.odt              # Detaljno uputstvo za CLI opcije
│   ├── graph-coloring-instructions.txt     # Kratko uputstvo za pokretanje Modula 1
│   └── spotify-analysis-instructions.txt   # Kratko uputstvo za pokretanje Modula 2
├── graph-coloring-rdd/                    # Modul 1 — Spark RDD API
│   ├── src/main/java/org/etf/graph/
│   │   ├── cli/                           # Parsiranje i validacija CLI argumenata
│   │   ├── config/                        # Konfiguracioni objekat izveden iz CLI opcija
│   │   ├── core/                          # Node (record), generisanje i validacija grafa, tajmer
│   │   ├── data/                          # (De)serijalizacija grafa/rezultata u JSON, izvoz metrika
│   │   ├── incremental/                   # Model promjena i inkrementalno bojenje (Zadatak 6)
│   │   ├── jobs/                          # Spark job-ovi: prosječan stepen, bojenje, validacija
│   │   └── metrics/                       # Strukture rezultata (records)
│   ├── run.sh                             # Skripta za pokretanje iz terminala
│   ├── cp.txt                             # Classpath zavisnosti (potrebno regenerisati lokalno)
│   └── pom.xml
├── spotify-analysis-dataframe/            # Modul 2 — Spark DataFrame API
│   ├── src/main/java/org/etf/spotify/analysis/
│   │   ├── analyzer/                      # 9 nezavisnih analiza (jedna klasa po analizi)
│   │   ├── config/                        # Konfiguracija Spark sesije
│   │   ├── exporter/                      # Izvoz rezultata u JSON + generisanje SUMMARY_REPORT.md
│   │   └── loader/                        # Eksplicitna šema, učitavanje i čišćenje CSV podataka
│   ├── data/dataset.csv                   # Spotify skup podataka
│   ├── results/                           # Generisani JSON rezultati + finalni izvještaj
│   └── pom.xml
└── .gitignore
```

Napomena: ovo su dva **nezavisna** Maven projekta (svaki sa svojim `pom.xml`-om), a ne moduli jednog agregatnog roditeljskog projekta — build i pokretanje se rade odvojeno za svaki od njih.

## Modul 1: Distribuirano bojenje grafa

Graf se generiše kao neusmjeren i netežinski (bez petlji), sa zadatim brojem čvorova i maksimalnim stepenom čvora (`GraphGenerator`). Svaki čvor je predstavljen kao nepromjenjiv (`record`) `Node` sa ID-jem, skupom susjeda i bojom, a cijeli graf se može serijalizovati/deserijalizovati u JSON.

### Algoritam

Bojenje grafa se izvršava iterativno nad `JavaPairRDD<Integer, Node>`, počevši od `K = maxStepen + 1` boja i smanjujući K sve dok se ne pronađe minimalan broj boja (hromatski broj) za koji je bojenje i dalje uspješno:

1. Filtriraju se neobojeni čvorovi.
2. Svaki neobojeni čvor prikuplja boje svojih obojenih susjeda i bira prvu slobodnu boju kao kandidata.
3. Kandidatska boja se najavljuje susjedima; ako dva susjedna čvora istovremeno biraju istu boju, čvor sa manjim ID-jem "pobjeđuje" (tie-breaking), a drugi ostaje neobojen za sljedeću iteraciju.
4. Koraci 1–3 se ponavljaju dok svi čvorovi ne budu obojeni (uspjeh) ili dok se za dvije uzastopne iteracije ne postigne napredak, odnosno dok neki čvor ne ostane bez kandidatske boje (neuspjeh za dati K).

Validacija (`GraphValidationJob`) nezavisno provjerava rezultat isključivo kroz Spark RDD operacije — broji parove susjednih čvorova koji dijele istu boju i po potrebi loguje uzorak konflikata.

### CLI opcije

| Opcija | Skraćenica | Opis |
|---|---|---|
| `--input <fajl>` | `-i` | Učitavanje grafa iz JSON fajla |
| `--generate <n>` | `-g` | Generisanje grafa sa `n` čvorova |
| `--max-degree <n>` | `-m` | Maksimalni stepen čvora pri generisanju |
| `--seed <n>` | `-s` | Seed za generisanje (podrazumijevano `123`) |
| `--partitions <n>` | — | Eksplicitan broj Spark particija |
| `--initial-colors <n>` | — | Početni broj boja K (umjesto `maxStepen + 1`) |
| `--run-average` | — | Proračun prosječnog i maksimalnog stepena grafa |
| `--run-coloring` | — | Pokretanje distribuiranog bojenja |
| `--run-validation` | — | Validacija ispravnosti bojenja |
| `--run-incremental` | — | Inkrementalno bojenje nakon promjena u grafu |
| `--use-baseline` | — | Koristi neoptimizovanu verziju algoritma (samo uz `--run-coloring`) |
| `--changes-file <fajl>` | — | JSON fajl sa promjenama za inkrementalno bojenje |
| `--measure-time` | — | Mjerenje vremena izvršavanja svih pokrenutih poslova |
| `--metrics-out <prefiks>` | — | Izvoz metrika u JSON (zahtijeva `--measure-time`) |
| `--output-graph <fajl>` | — | Snimanje kompletnog obojenog grafa u JSON |
| `--output-result <fajl>` | — | Snimanje kompaktnog rezultata bojenja (samo ID → boja) |
| `--help` | `-h` | Prikaz svih opcija |

Obavezan je bar jedan `--run-*` job, a graf mora biti definisan ili preko `--input` ili preko para `--generate` + `--max-degree`. `CLIParser` dodatno validira međusobnu kompatibilnost opcija (npr. `--use-baseline` se ne može kombinovati sa `--run-incremental`, `--output-result` zahtijeva prethodno bojenje, izlazna putanja ne smije biti ista kao ulazna).

### Optimizacije

Optimizovana verzija (`GraphColoringJob`) koristi iste korake algoritma kao baseline (`GraphColoringJobBaseline`), ali mijenja način na koji Spark izvršava posao:

| Aspekt | Baseline | Optimizovano |
|---|---|---|
| Agregacija boja susjeda | `groupByKey()` | `aggregateByKey()` — map-side combine, manje podataka kroz shuffle |
| Keširanje privremenih RDD-ova | `MEMORY_AND_DISK` | `MEMORY_ONLY_SER` — serijalizovano, manji memorijski otisak |
| Particionisanje | Podrazumijevano (`defaultParallelism`) | Podesivo preko `--partitions` |

`groupByKey()` mora prenijeti svaku pojedinačnu boju susjeda preko mreže, dok `aggregateByKey()` prvo lokalno kombinuje boje u `Set` na svakom Spark worker-u (uklanjajući duplikate) prije slanja — za čvor sa D susjeda i K boja (K << D), ovo teoretski smanjuje broj poruka, mrežni saobraćaj i memoriju po čvoru sa reda veličine O(D) na O(min(D, K)).

### Inkrementalno bojenje

`GraphDeltaManager` prima listu promjena (`ADD_NODE`, `REMOVE_NODE`, `ADD_EDGE`, `REMOVE_EDGE`) i za svaku:

1. Ažurira RDD grafa (dodaje/uklanja čvor ili ivicu).
2. Detektuje obuhvaćeni podgraf — čvorove pogođene promjenom.
3. Poništava boju samo pogođenim čvorovima i ponovo boji isključivo tu (redukovanu) listu, koristeći fiksni K dobijen iz inicijalnog bojenja, umjesto da cijeli graf boji ispočetka.

Ako se ne navede `--changes-file`, `MainApp` generiše skup demonstracionih promjena (provjerava da li dodavanje/brisanje čvora ili ivice ima smisla u datom grafu prije nego što je doda u listu promjena).

### Rezultati benchmark testiranja

Prosječna vremena izvršavanja kroz 10 pokretanja, sa identičnim brojem čvorova, maksimalnim stepenom i seed-om za oba scenarija:

| Test | Čvorova | Max. stepen | Baseline (ms) | Optimizovano (ms) | Poboljšanje | Broj poruka |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 500 | 3 | 4.512 | 4.467 | 1,0 % | 4.389 |
| 2 | 1.000 | 5 | 17.347 | 16.316 | 5,9 % | 20.567 |
| 3 | 1.500 | 8 | 71.999 | 62.097 | 13,8 % | 230.085 |
| 4 | 2.000 | 10 | 347.865 | 360.605 | −3,7 % | 128.609 |
| 5 | 2.500 | 12 | 557.554 | 538.009 | 3,5 % | 3.429.395 |
| 6 | 3.000 | 15 | 840.866 | 813.972 | 3,2 % | 1.766.176 |
| 7 | 3.500 | 18 | 1.234.935 | 1.167.944 | 5,4 % | 1.167.944 |
| 8 | 4.000 | 20 | 1.830.781 | 1.791.153 | 2,2 % | 832.014 |
| 9 | 4.500 | 22 | 2.303.447 | 2.275.801 | 1,2 % | 1.526.441 |
| 10 | 5.000 | 50 | 2.869.628 | 2.704.746 | 5,7 % | 6.521.798 |

Na ovim veličinama grafa prosječno poboljšanje je skromno (~3,8 %), a u jednom slučaju (Test 4) baseline je čak bio nešto brži — što je i očekivano, jer na manjim skupovima podataka režijski trošak Spark okvira (task scheduling, serijalizacija, koordinacija particija) nadmašuje uštedu u samom procesiranju. Optimizacije (manje shuffle-a, jeftinije keširanje) daju veći efekat kada graf ima više čvorova, veći prosječan stepen i kada se izvršava na pravom klasteru sa više worker-a — uslovi koji nisu u potpunosti zastupljeni u ovim lokalnim testovima.

## Modul 2: Analiza Spotify skupa podataka

### Šema podataka i čišćenje

Šema (nazivi kolona, tipovi, broj redova) odgovara javno dostupnom **Spotify Tracks Dataset**-u sa Kaggle-a. `DataLoader` definiše eksplicitnu šemu (umjesto oslanjanja na automatsku inferenciju tipova) i validira dataset prilikom učitavanja:

| Kolona | Tip | Opis |
|---|---|---|
| `track_id`, `artists`, `track_name`, `track_genre` | `String` | Obavezni identifikacioni podaci |
| `album_name` | `String` | Opciono |
| `popularity` | `Integer` | Popularnost [0, 100) |
| `duration_ms` | `Integer` | Trajanje pjesme u milisekundama |
| `explicit` | `Boolean` | Eksplicitan sadržaj |
| `danceability`, `energy`, `acousticness`, `instrumentalness`, `valence` | `Float` | Audio karakteristike, opseg [0.0, 1.0] |
| `key`, `mode`, `time_signature` | `Integer` | Tonalitet, mol/dur, takt (opciono) |
| `loudness`, `speechiness`, `liveness`, `tempo` | `Float` | Ostale audio karakteristike (opciono) |

Nakon učitavanja se uklanjaju redovi sa nedostajućim vrijednostima u ključnim kolonama i redovi kod kojih audio karakteristike izlaze iz opsega [0.0, 1.0]; učitani `DataFrame` se zatim kešira (`persist(MEMORY_AND_DISK)`) jer ga svih 9 analiza ponovo koristi.

### Analize

Sve analize rade isključivo preko Spark DataFrame API-ja i svaka svoj rezultat izvozi u poseban JSON fajl. Rezultati su iz priloženog `SUMMARY_REPORT.md` (dataset od 113.865 redova nakon čišćenja):

| # | Analiza | Šta se računa | Rezultat |
|---:|---|---|---|
| 1 | Distribucija podataka | Za svaku kolonu: mean/stddev/min/max/percentili (numeričke), top 10 vrijednosti (string), procentualna distribucija (boolean) | 20 analiziranih kolona |
| 2 | Kolaboracije | Top 10 kolaboracija između umjetnika (kolona `artists` razdvojena sa `;`) i prosječna razlika popularnosti solo vs. zajedničkih pjesama | 29.958 kolaboracija, prosječan efekat na popularnost: **−6,32** |
| 3 | Breakthrough pjesme | Pjesme sa popularnošću > 80 u albumima sa prosjekom < 50, poređenje audio karakteristika sa ostatkom albuma | 13 pronađenih pjesama, razlika u energiji: **+0,064** |
| 4 | Sweet spot tempa | Za top 5 žanrova: prosječna popularnost po opsezima tempa (<100, 100–120, >120 BPM) | 5 žanrova, prosječna prednost sweet spot opsega: **+3,83** |
| 5 | Eksplicitni sadržaj vs. popularnost | Korelacija eksplicitnosti i popularnosti po žanru | Pozitivan uticaj u 35 žanrova, negativan u 14, neutralan u 57 |
| 6 | Duge plesne pjesme | 10 najdužih pjesama sa plesivošću > 0,8, poređenje sa prosjekom žanra | Uticaj dužine na popularnost: **−16,31** |
| 7 | Eksplicitnost vs. valencija | Obrazac valencije kod eksplicitnih/neeksplicitnih pjesama po opsezima popularnosti širine 0,1 | 10 opsega, dominantan obrazac: neeksplicitne pjesme pozitivnije u 5/10 opsega |
| 8 | Dosljednost umjetnika | Umjetnici sa najnižom standardnom devijacijom popularnosti, veza sa žanrovskom specijalizacijom | 20 analiziranih umjetnika, 70 % (14) visoko specijalizovano |
| 9 | Akustične vs. vokalne | Poređenje prosječne popularnosti žanrova sa visokom akustičnošću/instrumentalnošću (>0,8) i vokalno teških žanrova | 58 akustičnih/instrumentalnih žanrova, 114 vokalnih, razlika popularnosti: **−1,16** |

### Rezime izvršavanja

| Metrika | Vrijednost |
|---|---|
| Veličina dataseta (nakon čišćenja) | 113.865 redova |
| Ukupno vrijeme izvršavanja | 36,07 s |
| Prosječno vrijeme po analizi | 4,01 s |
| Najsporija analiza | Distribucija podataka — 14,60 s (40,5 % ukupnog vremena) |

Aplikacija generiše 9 pojedinačnih JSON fajlova (`results/01_*.json` – `results/09_*.json`) i jedan zbirni `results/SUMMARY_REPORT.md` sa vremenima izvršavanja i sažetkom svake analize.

## Tehnologije i alati

| Kategorija | Modul 1 (bojenje grafa) | Modul 2 (Spotify analiza) |
|---|---|---|
| Jezik | Java 17 | Java 17 |
| Apache Spark | 3.5.4 (Core + SQL) | 3.5.0 (Core + SQL) |
| Build alat | Maven (`maven-shade-plugin` za fat jar) | Maven (`maven-shade-plugin` za fat jar) |
| Serijalizacija | Jackson, Gson | Gson |
| CLI | `commons-cli` | — (jedini argument je putanja do CSV-a) |
| Logovanje | Log4j2 (preko SLF4J) | Log4j2 (preko SLF4J) |
| Podaci | — | Spotify Tracks Dataset (Kaggle), CSV, 114.000 pjesama, 20 kolona |

## Kako pokrenuti projekat lokalno

### Preduslovi

- Instaliran **JDK 17**
- Instaliran **Apache Maven**

### Modul 1 — Distribuirano bojenje grafa

```bash
cd graph-coloring-rdd
mvn compile
```

Repozitorijum sadrži `cp.txt` sa spiskom zavisnosti, ali je generisan na razvojnoj mašini autora (apsolutne putanje do lokalnog `.m2` repozitorijuma), pa ga je potrebno regenerisati na svom računaru prije prvog pokretanja:

```bash
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
```

Nakon toga, pokretanje ide preko priložene skripte:

```bash
chmod +x run.sh
./run.sh --generate 500 --max-degree 3 --run-coloring --run-incremental
```

ili direktno iz terminala (potrebno zbog Java modula koje Spark 3.5.x otvara refleksijom):

```bash
java --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED \
     --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
     --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens java.base/sun.nio.cs=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED \
     --add-opens java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.security.jgss/sun.security.krb5=ALL-UNNAMED \
     -cp "target/classes:$(cat cp.txt)" org.etf.graph.MainApp --generate 500 --max-degree 3 --run-coloring
```

Nekoliko reprezentativnih primjera (kompletan spisak opcija: `./run.sh --help`):

```bash
# Bojenje sa validacijom i mjerenjem vremena
./run.sh --generate 1000 --max-degree 15 --seed 42 \
         --run-coloring --run-validation --measure-time --metrics-out experiment1

# Poređenje baseline i optimizovane verzije
./run.sh --input graph.json --run-coloring --use-baseline --measure-time

# Inkrementalno bojenje sa sopstvenim promjenama i custom brojem particija
./run.sh --input graph.json --run-coloring --run-incremental \
         --changes-file changes.json --partitions 32 --output-graph final_graph.json
```

### Modul 2 — Analiza Spotify skupa podataka

```bash
cd spotify-analysis-dataframe
mvn package
java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED \
     --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED \
     --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     -jar target/spotify-data-analysis-1.0-SNAPSHOT.jar data/dataset.csv
```

Alternativno, pokretanje iz IDE-a: glavna klasa `SpotifyAnalysisApp`, argument programa `data/dataset.csv`, uz iste `--add-opens` VM opcije i, po želji, `-Dlog4j2.configurationFile=file:src/main/resources/log4j2.xml`. Rezultati (9 JSON fajlova + `SUMMARY_REPORT.md`) se generišu u `results/`.
