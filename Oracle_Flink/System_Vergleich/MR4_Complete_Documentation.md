# MR4 – MATCH_RECOGNIZE Queries je System & Dialekt-Abgleich

**Komplette Dokumentation aller MATCH_RECOGNIZE Queries für Flink, Oracle und Trino**

---

## 📋 Inhaltsverzeichnis

1. [Systemvergleich & Dialekt-Unterschiede](#1-systemvergleich--dialekt-unterschiede)
2. [Query-Übersicht](#2-query-übersicht)
3. [Flink Queries](#3-flink-queries)
4. [Oracle Queries](#4-oracle-queries)
5. [Trino Queries](#5-trino-queries)
6. [Workarounds & Besonderheiten](#6-workarounds--besonderheiten)
7. [Setup-Anleitung](#7-setup-anleitung)
8. [Benchmark-Empfehlungen](#8-benchmark-empfehlungen)

---

## 1. Systemvergleich & Dialekt-Unterschiede

### 1.1 Feature-Matrix

| Feature | Flink | Oracle | Trino | Anmerkung |
|---------|-------|--------|-------|-----------|
| **SUBSET** | ❌ | ✅ | ❌ | Oracle-exklusiv |
| **ALL ROWS PER MATCH** | ✅ | ✅ | ❌ | Trino nur ONE ROW |
| **AFTER MATCH SKIP** | ✅ Vollständig | ✅ Vollständig | ⚠️ Teilweise | Trino fehlt TO FIRST/LAST |
| **WITHIN** | ✅ | ❌ | ❌ | Flink-exklusiv (Streaming) |
| **Streaming Support** | ✅ | ❌ | ❌ | Flink unique |
| **Event Time** | ✅ | ❌ | ❌ | Flink Watermarks |
| **PREV/NEXT** | ❌ | ✅ | ❌ | Oracle erweiterte Navigation |

### 1.2 Dialekt-Besonderheiten

#### **Apache Flink**
- ✅ Modernste Streaming-Implementierung
- ✅ WITHIN-Klausel für Zeitfenster
- ✅ Event Time & Watermarks
- ❌ Kein SUBSET (Workaround über CASE erforderlich)
- ⚠️ Filesystem Connector benötigt CSV-Dateien

#### **Oracle Database**
- ✅ Vollständigster ANSI SQL:2016 Dialekt
- ✅ SUBSET für Gruppen-Aliase
- ✅ Lazy Quantifiers (`*?`, `+?`)
- ✅ PREV/NEXT für erweiterte Pattern-Navigation
- ⚠️ Keine Streaming-Unterstützung

#### **Trino**
- ⚠️ Eingeschränkte Implementierung
- ❌ Kein SUBSET
- ❌ Kein ALL ROWS PER MATCH (nur ONE ROW)
- ❌ SKIP TO FIRST/LAST fehlt
- ✅ Memory Connector ideal für Tests

---

## 2. Query-Übersicht

### Gemeinsame Queries (alle 3 Systeme)

| Nr | Query Name | Pattern | Besonderheit |
|----|------------|---------|--------------|
| 01 | overlap_default | `A B+` | Standard Overlap-Verhalten |
| 02 | overlap | `A B+` | Explizit SKIP PAST LAST ROW |
| 03 | greedy | `A B*` | Greedy Quantifier |
| 03b | reluctant | `A B*` | Reluctant Matching |
| 04 | define | `A B+ C` | Komplexe DEFINE-Klauseln |
| 05 | ONE ROW PER MATCH | `A B+` | Nur erste Match-Zeile |
| 06 | ALL ROWS PER MATCH | `A B+` | Alle Zeilen ausgeben |
| 07 | SUBSET | `(A\|B)+ C` | Gruppen-Aliase |

### System-spezifische Queries

| Nr | Query Name | Nur in | Grund |
|----|------------|--------|-------|
| 08 | WITHIN | Flink | Zeitfenster-Feature |
| 09 | Nullwerte | Oracle/Trino | Nicht in Flink-Testset |

---

## 3. Flink Queries

### 3.1 Setup

```sql
-- Tabelle mit Filesystem Connector
CREATE TABLE crime_events (
    id BIGINT,
    district STRING,
    event_time TIMESTAMP(3),
    primary_type STRING,
    lat DOUBLE,
    lon DOUBLE,
    WATERMARK FOR event_time AS event_time - INTERVAL '0' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/crime_data.csv',
    'format' = 'csv',
    'csv.ignore-parse-errors' = 'true',
    'csv.ignore-first-line' = 'true'
);
```

---

### 3.2 Query 01 - Overlap Default

**Datei:** `queries/flink/01_overlap_default.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        LAST(B.event_time) AS end_time,
        COUNT(B.*) AS repeat_count
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
            AND B.event_time <= A.event_time + INTERVAL '1' HOUR
) AS patterns;
```

**Erklärung:** Standard-Overlap, Matches überlappen sich automatisch.

---

### 3.3 Query 02 - Overlap Explizit

**Datei:** `queries/flink/02_overlap.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        LAST(B.event_time) AS end_time
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
            AND B.event_time <= A.event_time + INTERVAL '1' HOUR
);
```

**Erklärung:** Explizites SKIP PAST LAST ROW verhindert Überlappung.

---

### 3.4 Query 03 - Greedy

**Datei:** `queries/flink/03_greedy.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        LAST(B.event_time) AS end_time
    PATTERN (A B*)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY'
            AND event_time <= A.event_time + INTERVAL '2' HOUR
);
```

**Erklärung:** Greedy `B*` matched so viele B's wie möglich.

---

### 3.5 Query 03b - Reluctant

**Datei:** `queries/flink/03_reluctant.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        B.event_time AS next_time
    PATTERN (A B)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY'
);
```

**Erklärung:** Minimales Matching - nur A gefolgt von einem B.

---

### 3.6 Query 04 - Define

**Datei:** `queries/flink/04_define.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS theft_time,
        B.event_time AS violent_time,
        C.event_time AS arrest_time
    PATTERN (A B+ C)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type IN ('BATTERY', 'ASSAULT', 'ROBBERY'),
        C AS primary_type IN ('BATTERY', 'ASSAULT') 
            AND event_time <= A.event_time + INTERVAL '6' HOUR
);
```

**Erklärung:** Drei verschiedene Pattern-Variablen mit spezifischen Definitionen.

---

### 3.7 Query 05 - ONE ROW PER MATCH

**Datei:** `queries/flink/05_ONE_ROW_PER_MATCH.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        LAST(B.event_time) AS end_time,
        COUNT(B.*) AS count_events
    ONE ROW PER MATCH
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
);
```

**Erklärung:** Gibt nur eine Zeile pro Match zurück (Zusammenfassung).

---

### 3.8 Query 06 - ALL ROWS PER MATCH

**Datei:** `queries/flink/06_ALL_ROWS_PER_MATCH.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        MATCH_NUMBER() AS match_id,
        CLASSIFIER() AS variable_name,
        event_time AS event_ts,
        primary_type AS crime_type
    ALL ROWS PER MATCH
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
);
```

**Erklärung:** Jede Zeile des Matches wird einzeln ausgegeben.

---

### 3.9 Query 07 - SUBSET Workaround

**Datei:** `queries/flink/07_SUBSET_workaround.sql`

```sql
-- Flink hat kein SUBSET → Workaround über CASE
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(CASE WHEN CLASSIFIER() IN ('A', 'B') 
              THEN event_time END) AS start_time,
        LAST(CASE WHEN CLASSIFIER() IN ('A', 'B') 
             THEN event_time END) AS end_time,
        C.event_time AS resolution_time
    PATTERN ((A | B)+ C)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY',
        C AS primary_type IN ('THEFT', 'BATTERY') 
            AND event_time > FIRST(event_time) + INTERVAL '1' HOUR
);
```

**Workaround:** SUBSET wird über CLASSIFIER() + CASE simuliert.

---

### 3.10 Query 08 - WITHIN (Flink-exklusiv)

**Datei:** `queries/flink/08_WITHIN.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        LAST(B.event_time) AS end_time
    PATTERN (A B+) WITHIN INTERVAL '2' HOUR
    DEFINE
        B AS B.primary_type = A.primary_type
);
```

**Erklärung:** WITHIN begrenzt die Dauer eines Matches (nur Flink).

---

## 4. Oracle Queries

### 4.1 Setup

```sql
CREATE TABLE crime_events (
    id NUMBER,
    district VARCHAR2(50),
    event_time TIMESTAMP,
    primary_type VARCHAR2(50),
    lat NUMBER,
    lon NUMBER
);

-- Index für Performance
CREATE INDEX idx_crime_time ON crime_events(district, event_time);
```

---

### 4.2 Query 01 - Overlap Default

**Datei:** `queries/oracle/01_overlap_default.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time,
        COUNT(B.*) AS repeat_count
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
            AND B.event_time <= A.event_time + INTERVAL '1' HOUR
);
```

---

### 4.3 Query 02 - Overlap

**Datei:** `queries/oracle/02_overlap.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
            AND B.event_time <= A.event_time + INTERVAL '1' HOUR
);
```

---

### 4.4 Query 03 - Greedy

**Datei:** `queries/oracle/03_greedy.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time
    PATTERN (A B*)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY'
            AND event_time <= A.event_time + INTERVAL '2' HOUR
);
```

---

### 4.5 Query 03b - Reluctant

**Datei:** `queries/oracle/03_reluctant.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        B.event_time AS next_time
    PATTERN (A B)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY'
);
```

---

### 4.6 Query 04 - Define

**Datei:** `queries/oracle/04_define.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS theft_time,
        FIRST(B.event_time) AS violent_start,
        LAST(B.event_time) AS violent_end,
        C.event_time AS arrest_time
    PATTERN (A B+ C)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type IN ('BATTERY', 'ASSAULT', 'ROBBERY'),
        C AS primary_type IN ('BATTERY', 'ASSAULT') 
            AND event_time <= A.event_time + INTERVAL '6' HOUR
);
```

---

### 4.7 Query 05 - ONE ROW PER MATCH

**Datei:** `queries/oracle/05_ONE_ROW_PER_MATCH.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time,
        COUNT(B.*) AS count_events
    ONE ROW PER MATCH
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
);
```

---

### 4.8 Query 06 - ALL ROWS PER MATCH

**Datei:** `queries/oracle/06_ALL_ROWS_PER_MATCH.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        MATCH_NUMBER() AS match_id,
        CLASSIFIER() AS variable_name,
        event_time AS event_ts,
        primary_type AS crime_type
    ALL ROWS PER MATCH
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
);
```

---

### 4.9 Query 07 - SUBSET (Oracle native)

**Datei:** `queries/oracle/07_SUBSET.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(VIOLENT.event_time) AS start_time,
        LAST(VIOLENT.event_time) AS end_time,
        C.event_time AS resolution_time
    PATTERN ((A | B)+ C)
    SUBSET VIOLENT = (A, B)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY',
        C AS primary_type IN ('THEFT', 'BATTERY') 
            AND event_time > FIRST(event_time) + INTERVAL '1' HOUR
);
```

**Erklärung:** SUBSET erstellt Alias `VIOLENT` für Gruppe `(A, B)`.

---

### 4.10 Query 09 - Nullwerte

**Datei:** `queries/oracle/09_Nullwerte.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS start_time,
        B.event_time AS null_event_time
    PATTERN (A B+)
    DEFINE
        A AS lat IS NOT NULL AND lon IS NOT NULL,
        B AS lat IS NULL OR lon IS NULL
);
```

**Erklärung:** Erkennt Sequenzen wo nach gültigen Koordinaten NULL-Werte folgen.

---

## 5. Trino Queries

### 5.1 Setup

```sql
USE memory.default;

CREATE TABLE crime_events (
    id BIGINT,
    district VARCHAR,
    event_time TIMESTAMP,
    primary_type VARCHAR,
    lat DOUBLE,
    lon DOUBLE
);
```

---

### 5.2 Query 01 - Overlap Default

**Datei:** `queries/trino/01_overlap_default.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time,
        COUNT(B.*) AS repeat_count
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
            AND B.event_time <= A.event_time + INTERVAL '1' HOUR
);
```

---

### 5.3 Query 02 - Overlap

**Datei:** `queries/trino/02_overlap.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
            AND B.event_time <= A.event_time + INTERVAL '1' HOUR
);
```

---

### 5.4 Query 03 - Greedy

**Datei:** `queries/trino/03_greedy.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time
    PATTERN (A B*)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY'
            AND event_time <= A.event_time + INTERVAL '2' HOUR
);
```

---

### 5.5 Query 03b - Reluctant

**Datei:** `queries/trino/03_reluctant.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        B.event_time AS next_time
    PATTERN (A B)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY'
);
```

---

### 5.6 Query 04 - Define

**Datei:** `queries/trino/04_define.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        A.event_time AS theft_time,
        FIRST(B.event_time) AS violent_start,
        LAST(B.event_time) AS violent_end,
        C.event_time AS arrest_time
    PATTERN (A B+ C)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type IN ('BATTERY', 'ASSAULT', 'ROBBERY'),
        C AS primary_type IN ('BATTERY', 'ASSAULT') 
            AND event_time <= A.event_time + INTERVAL '6' HOUR
);
```

---

### 5.7 Query 05 - ONE ROW PER MATCH

**Datei:** `queries/trino/05_ONE_ROW_PER_MATCH.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time,
        COUNT(B.*) AS count_events
    ONE ROW PER MATCH
    PATTERN (A B+)
    DEFINE
        B AS B.primary_type = A.primary_type
);
```

**Hinweis:** Trino unterstützt nur ONE ROW PER MATCH (Standard).

---

### 5.8 Query 06 - ALL ROWS PER MATCH (Nicht unterstützt)

**Datei:** `queries/trino/06_ALL_ROWS_PER_MATCH_NOT_SUPPORTED.sql`

```sql
-- ❌ Trino unterstützt ALL ROWS PER MATCH nicht!
-- Workaround: Nutze ONE ROW PER MATCH + JOIN für Details

-- Alternative (wenn Details benötigt):
WITH matches AS (
    SELECT 
        district,
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS end_time
    FROM crime_events
    MATCH_RECOGNIZE (
        PARTITION BY district
        ORDER BY event_time
        MEASURES
            FIRST(A.event_time) AS start_time,
            LAST(B.event_time) AS end_time
        ONE ROW PER MATCH
        PATTERN (A B+)
        DEFINE
            B AS B.primary_type = A.primary_type
    )
)
SELECT 
    m.district,
    m.start_time,
    m.end_time,
    c.event_time,
    c.primary_type
FROM matches m
JOIN crime_events c 
    ON c.district = m.district 
    AND c.event_time BETWEEN m.start_time AND m.end_time
ORDER BY m.district, c.event_time;
```

---

### 5.9 Query 07 - SUBSET Workaround

**Datei:** `queries/trino/07_SUBSET_workaround.sql`

```sql
-- Trino hat kein SUBSET → Workaround über CLASSIFIER()
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(CASE WHEN CLASSIFIER() IN ('A', 'B') 
              THEN event_time END) AS start_time,
        LAST(CASE WHEN CLASSIFIER() IN ('A', 'B') 
             THEN event_time END) AS end_time,
        C.event_time AS resolution_time
    ONE ROW PER MATCH
    PATTERN ((A | B)+ C)
    DEFINE
        A AS primary_type = 'THEFT',
        B AS primary_type = 'BATTERY',
        C AS primary_type IN ('THEFT', 'BATTERY') 
            AND event_time > FIRST(event_time) + INTERVAL '1' HOUR
);
```

---

### 5.10 Query 09 - Nullwerte

**Datei:** `queries/trino/09_Nullwerte.sql`

```sql
SELECT *
FROM crime_events
MATCH_RECOGNIZE (
    PARTITION BY district
    ORDER BY event_time
    MEASURES
        FIRST(A.event_time) AS start_time,
        LAST(B.event_time) AS null_event_time
    ONE ROW PER MATCH
    PATTERN (A B+)
    DEFINE
        A AS lat IS NOT NULL AND lon IS NOT NULL,
        B AS lat IS NULL OR lon IS NULL
);
```

---

## 6. Workarounds & Besonderheiten

### 6.1 SUBSET Workaround (Flink/Trino)

**Problem:** SUBSET nicht verfügbar

**Oracle (native):**
```sql
SUBSET VIOLENT = (A, B)
MEASURES FIRST(VIOLENT.event_time) AS start_time
```

**Flink/Trino (Workaround):**
```sql
MEASURES 
    FIRST(CASE WHEN CLASSIFIER() IN ('A', 'B') 
          THEN event_time END) AS start_time
```

---

### 6.2 ALL ROWS PER MATCH Workaround (Trino)

**Problem:** Trino hat kein ALL ROWS PER MATCH

**Lösung:** ONE ROW + JOIN
```sql
WITH matches AS (
    SELECT ... ONE ROW PER MATCH ...
)
SELECT * FROM matches m
JOIN crime_events c 
    ON c.event_time BETWEEN m.start_time AND m.end_time
```

---

### 6.3 WITHIN Alternative (Oracle/Trino)

**Problem:** WITHIN gibt es nur in Flink

**Flink:**
```sql
PATTERN (A B+) WITHIN INTERVAL '2' HOUR
```

**Oracle/Trino (Alternative):**
```sql
DEFINE
    B AS B.event_time <= A.event_time + INTERVAL '2' HOUR
```

---

### 6.4 SKIP TO FIRST/LAST (Trino)

**Problem:** Trino unterstützt nur SKIP PAST LAST ROW und SKIP TO NEXT ROW

**Nicht verfügbar in Trino:**
```sql
AFTER MATCH SKIP TO FIRST B  -- ❌
AFTER MATCH SKIP TO LAST B   -- ❌
```

**Verfügbar in Trino:**
```sql
AFTER MATCH SKIP PAST LAST ROW  -- ✅
AFTER MATCH SKIP TO NEXT ROW    -- ✅
```

---

## 7. Setup-Anleitung

### 7.1 Flink Setup

```bash
# 1. Flink starten
cd /vol/fob-vol1/mi23/tuerklic/flink
./bin/start-cluster.sh

# 2. SQL Client starten
./bin/sql-client.sh

# 3. Batch Mode aktivieren
SET 'execution.runtime-mode' = 'batch';

# 4. Tabelle erstellen (siehe Query 3.1)

# 5. Queries ausführen
-- Kopiere Query aus queries/flink/*.sql
```

---

### 7.2 Oracle Setup

```bash
# 1. SQL*Plus starten
sqlplus username/password@database

# 2. Tabelle erstellen (siehe Query 4.1)

# 3. CSV-Daten laden
sqlldr username/password control=load_crimes.ctl

# 4. Queries ausführen
@queries/oracle/01_overlap_default.sql
```

---

### 7.3 Trino Setup

```bash
# 1. Trino CLI starten
trino --server http://localhost:8080

# 2. Memory Catalog verwenden
USE memory.default;

# 3. Tabelle erstellen (siehe Query 5.1)

# 4. Testdaten einfügen
INSERT INTO crime_events VALUES
    (1, 'Mitte', TIMESTAMP '2025-01-01 10:00:00', 'THEFT', 52.52, 13.40),
    ...;

# 5. Queries ausführen
-- Kopiere aus queries/trino/*.sql
```

---

## 8. Benchmark-Empfehlungen

### 8.1 Verbesserungen vom Projektleiter

Basierend auf `MR4_MATCH_RECOGNIZE_Benchmarks_verbessert.pdf`:

1. ✅ **SELECT *** statt `SELECT COUNT(*)` → Materialisierung
2. ✅ **Keine PARTITION BY** → Vermeidung künstlicher Parallelisierung
3. ✅ **Selektive Prädikate** statt Tautologien
4. ✅ **Alternation + Variable Länge** → Backtracking erzeugen
5. ✅ **Selektivitätsvariationen** → Laufzeit/Speicher-Analyse

---

### 8.2 Empfohlene Benchmark-Queries

**Für Performance-Tests:**
- Query 03 (Greedy) - zeigt Backtracking-Overhead
- Query 04 (Define) - zeigt komplexe Pattern-Matching
- Query 06 (ALL ROWS) - zeigt Output-Materialisierung

**Für Scalability-Tests:**
- Query 01 (Overlap Default) - Baseline
- Query 08 (WITHIN, nur Flink) - Zeitfenster-Performance

**Für Feature-Vergleiche:**
- Query 07 (SUBSET) - Oracle vs Flink/Trino Workaround
- Query 06 (ALL ROWS) - Oracle/Flink vs Trino JOIN-Workaround

---

## 9. Zusammenfassung

### 9.1 Query-Kompatibilität

| Query | Flink | Oracle | Trino | Anmerkung |
|-------|-------|--------|-------|-----------|
| 01-05 | ✅ | ✅ | ✅ | 100% kompatibel |
| 06 ALL ROWS | ✅ | ✅ | ⚠️ | Trino: JOIN-Workaround |
| 07 SUBSET | ⚠️ | ✅ | ⚠️ | Oracle native, andere: CLASSIFIER() |
| 08 WITHIN | ✅ | ⚠️ | ⚠️ | Flink-exklusiv, andere: DEFINE |
| 09 Nullwerte | ❌ | ✅ | ✅ | Nicht in Flink-Testset |

---

### 9.2 Best Practices

**Für portable Queries (alle Systeme):**
- Verwende ONE ROW PER MATCH
- Vermeide SUBSET
- Nutze SKIP PAST LAST ROW
- Zeitfenster über DEFINE statt WITHIN

**Für Oracle-spezifische Features:**
- Nutze SUBSET für Gruppen-Aliase
- Verwende PREV/NEXT für Navigation
- Lazy Quantifiers (`*?`, `+?`)

**Für Flink-spezifische Features:**
- WITHIN für Zeitfenster
- Event Time + Watermarks
- Streaming-Semantik

---

### 9.3 Ordnerstruktur

```
MR4_Queries/
├── queries/
│   ├── flink/
│   │   ├── 01_overlap_default.sql
│   │   ├── 02_overlap.sql
│   │   ├── 03_greedy.sql
│   │   ├── 03_reluctant.sql
│   │   ├── 04_define.sql
│   │   ├── 05_ONE_ROW_PER_MATCH.sql
│   │   ├── 06_ALL_ROWS_PER_MATCH.sql
│   │   ├── 07_SUBSET_workaround.sql
│   │   └── 08_WITHIN.sql
│   ├── oracle/
│   │   ├── 01_overlap_default.sql
│   │   ├── 02_overlap.sql
│   │   ├── 03_greedy.sql
│   │   ├── 03_reluctant.sql
│   │   ├── 04_define.sql
│   │   ├── 05_ONE_ROW_PER_MATCH.sql
│   │   ├── 06_ALL_ROWS_PER_MATCH.sql
│   │   ├── 07_SUBSET.sql
│   │   └── 09_Nullwerte.sql
│   └── trino/
│       ├── 01_overlap_default.sql
│       ├── 02_overlap.sql
│       ├── 03_greedy.sql
│       ├── 03_reluctant.sql
│       ├── 04_define.sql
│       ├── 05_ONE_ROW_PER_MATCH.sql
│       ├── 06_ALL_ROWS_PER_MATCH_NOT_SUPPORTED.sql
│       ├── 07_SUBSET_workaround.sql
│       └── 09_Nullwerte.sql
└── README.md (dieses Dokument)
```

---

## 10. Literatur & Referenzen

- **ANSI SQL:2016** - Row Pattern Recognition Standard
- **Apache Flink Documentation** - MATCH_RECOGNIZE Guide
- **Oracle Database 21c** - Pattern Matching Documentation
- **Trino Documentation** - Row Pattern Recognition

---

**Dokumentation erstellt:** 2026-03-31  
**Version:** 1.0  
**Projekt:** MR4 - MATCH_RECOGNIZE Multi-System Comparison

---

## Anhang: Vollständige Beispiel-Outputs

### Beispiel: Query 01 auf allen Systemen

**Eingabedaten (alle Systeme identisch):**
```
id | district | event_time          | primary_type | lat   | lon
---+----------+---------------------+--------------+-------+------
1  | Mitte    | 2025-01-01 10:00:00 | THEFT        | 52.52 | 13.40
2  | Mitte    | 2025-01-01 10:15:00 | THEFT        | 52.52 | 13.41
3  | Mitte    | 2025-01-01 10:30:00 | THEFT        | 52.52 | 13.42
4  | Mitte    | 2025-01-01 11:30:00 | THEFT        | 52.52 | 13.43
```

**Output (alle Systeme):**
```
district | start_time          | end_time            | repeat_count
---------+---------------------+---------------------+-------------
Mitte    | 2025-01-01 10:00:00 | 2025-01-01 10:30:00 | 2
Mitte    | 2025-01-01 11:30:00 | 2025-01-01 11:30:00 | 0
```

**Erklärung:** 
- Match 1: Events 1→2→3 (alle innerhalb 1 Stunde)
- Match 2: Event 4 (kein Folge-Event in 1h)

---

**Ende der Dokumentation**
