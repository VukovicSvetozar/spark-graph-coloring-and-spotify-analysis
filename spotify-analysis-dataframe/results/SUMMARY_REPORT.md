# SPOTIFY DATA ANALYSIS - Finalni Izvještaj

**Datum:** Sat Dec 20 19:46:21 CET 2025
**Projekat:** Odabrana Poglavlja Operativnih Sistema
**Framework:** Apache Spark DataFrame API (3.5.0)
**Jezik:** Java 11+

---

## Rezime Izvršavanja

| Metrika                             | Vrednost             |
|-------------------------------------|----------------------|
| **Dataset veličina**                | 113,865 redova       |
| **Ukupno vrijeme**                  | 36.07 sekundi        |
| **Prosječno vrijeme po analizi**    | 4.01 sekundi         |
| **Broj analiza**                    | 9 (1 + 8 dodatnih)   |
| **Ukupno bodova**                   | 20 (4 + 8×2)         |

### Pojedinačna Vremena Izvršavanja

| #   | Analiza                             | Vrijeme      | % Ukupnog  |
|-----|-------------------------------------|--------------|------------|
| 1   | Analiza distribucije podataka       | 14.60 s      | 40.5%      |
| 2   | Analiza kolaboracija                | 0.93 s       | 2.6%       |
| 3   | Breakthrough pjesme analiza         | 2.74 s       | 7.6%       |
| 4   | Sweet spot tempo analiza            | 1.59 s       | 4.4%       |
| 5   | Eksplicitni sadržaj korelacija      | 1.68 s       | 4.7%       |
| 6   | Duge plesne pjesme analiza          | 0.77 s       | 2.1%       |
| 7   | Eksplicitnost vs valencija          | 0.90 s       | 2.5%       |
| 8   | Dosljednost umjetnika               | 1.27 s       | 3.5%       |
| 9   | Akustične vs vokalne pjesme         | 1.32 s       | 3.7%       |

**Napomena:** Ukupno vrijeme analiza: 25.79 s

---

## Pregled Analiza

### 1. Analiza Distribucije Podataka

```
Ukupno kolona: 20
Detalji: `01_distribution_analysis.json`
```

### 2. Top 10 Kolaboracija

```
Analizirano kolaboracija: 29958
Prosječan boost popularnosti: -6.32
Detalji: `02_collaboration_analysis.json`
```

### 3. Breakthrough Pjesme

```
Pronađeno breakthrough pjesama: 13
Razlika u energiji (vs ostale u albumu): 0.064
Detalji: `03_breakthrough_songs.json`
```

### 4. Sweet Spot Tempo Analiza

```
Analizirano žanrova: 5
Sweet spot identifikovan za svaki žanr
Prosječna prednost sweet spot-a: 3.83
Detalji: `04_tempo_sweetspot.json`
```

### 5. Eksplicitni Sadržaj Korelacija

```
Pozitivan uticaj u 35 žanrova
Negativan uticaj u 14 žanrova
Neutralan uticaj u 57 žanrova
Detalji: `05_explicit_content_correlation.json`
```

### 6. Duge Plesne Pjesme (2 boda)

```
Analizirano pjesama: 10
Uticaj dužine: -16.31
Detalji: `06_long_danceable_tracks.json`
```

### 7. Eksplicitnost vs Valencija

```
Analizirano opsega: 10
Dominantan obrazac: NonExplicit_More_Positive (5/10)
Detalji: `07_explicitness_valence.json`
```

### 8. Dosljednost Umjetnika (2 boda)

```
Analizirano umjetnika: 20
Visoko specijalizovanih: 14 (70.0%)
Detalji: `08_artist_consistency.json`
```

### 9. Akustične vs Vokalne

```
Akustični/instrumentalni žanrova: 58
Vokalnih žanrova: 114
Razlika popularnosti: -1.16
Detalji: `09_acoustic_vs_vocal.json`
```

---

## Zaključak

Sve analize uspješno izvršene koristeći **Apache Spark DataFrame API**.
Rezultati su dostupni u odvojenim JSON fajlovima za svaki zadatak.

**Generisani fajlovi:**

- **9 × JSON** fajlova (jedan po analizi)
- **1 × SUMMARY_REPORT.md** (ovaj fajl)

---

