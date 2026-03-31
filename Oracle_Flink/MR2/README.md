# MATCH_RECOGNIZE Korrektheitstest-Suite

## Überblick: 
Diese Test-Suite überprüft die korrekte Semantik von SQL MATCH_RECOGNIZE anhand kleiner, gezielter Datensätze. 
Jeder Testfall fokussiert einen bestimmten Aspekt des Pattern-Matchings und ist vollständig reproduzierbar. 

## Struktur: 
* datasets/tiny/ - Eingabedaten (CSV)
* tests/ - SQL-Queries
* expected/ - Erwartete Ergebnisse

Ein Test besteht jeweils aus: 
* Dataset
* Query
* Expected Output

## Testfälle
* Overlapping_default
* Overlapping
* Greedy vs. Reluctant
* DEFINE
* ONE ROW / ALL ROWS PER MATCH
* SUBSET
* WITHIN
* NULL-Werte

## Ausführung
* Dataset laden (Tabellenname je nach Query unterschiedlich)
* Query ausführen 
* Ergebnis mit expected/ vergleichen 

## Evaluation
Die Testfälle wurden mit folgenden Systemen ausgeführt:
* Apache Flink
* Trino
* Oracle
Alle Systeme lieferten Ergebnisse, die mit den erwarteten Resultaten übereinstimmen.

