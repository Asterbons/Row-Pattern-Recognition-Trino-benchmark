# MATCH_RECOGNIZE  MR6 Oracle

Jede Query wurde auf den vorbereiteten Datensätzen ausgeführt, wobei die Ergebnisse in einer eigenen Tabelle gespeichert wurden (z. B. CREATE TABLE mr1 AS …). Vor jedem Lauf wurde die Zieltabelle gelöscht, um sicherzustellen, dass jede Wiederholung unabhängig ist.

Die Laufzeiten wurden mit SET TIMING ON erfasst und zusätzlich in einer separaten Performance-Tabelle dokumentiert. Die CPU-Zeit wurde innerhalb eines PL/SQL-Blocks mittels DBMS_UTILITY.GET_CPU_TIME gemessen, wobei die Differenz vor und nach der Query-Ausführung gebildet wurde.

Jede Kombination aus Query und Datensatz wurde fünfmal wiederholt, um Median- und Quartilwerte berechnen zu können. Die Rohdaten wurden anschließend als JSON exportiert, inklusive Metadaten wie System (Oracle), Version, Datensatzgröße, Konfiguration, verwendete Messmethoden, Einheiten und Anzahl der Wiederholungen.

Ein dedizierter Benchmark-Code wurde nicht verwendet; stattdessen erfolgte die Messung direkt innerhalb der Datenbankumgebung unter Verwendung der oben beschriebenen Mechanismen. Die Ausführungspläne wurden hinsichtlich Parallelisierung überprüft. Dabei zeigten sich keine Hinweise auf Parallel Query Execution (keine PX-Operatoren), sodass die Abfragen sequenziell ausgeführt wurden. Eine Parallelisierung wurde nicht konfiguriert.