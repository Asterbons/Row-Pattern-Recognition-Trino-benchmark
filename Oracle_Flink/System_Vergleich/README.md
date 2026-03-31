# Flink/Oracle/Trino Scalability Benchmark Visualization

## 📦 Paketinhalt

Dieses Paket enthält alle Tools zur Visualisierung der MATCH_RECOGNIZE Scalability-Benchmarks:

```
Scalability_Visualization/
├── README.md                              # Diese Datei
├── data/                                  # JSON-Dateien mit Benchmark-Ergebnissen
│   ├── scalability_flink_final.json      # Flink: 1K-10M, alle Metriken
│   ├── scalability_oracle_final.json     # Oracle: 1K-10M
│   └── scalability_trino_final.json      # Trino: 1K-10M
├── visualizers/                           # HTML Visualisierungs-Tools
│   ├── flink_scalability_visualizer.html # Nur Flink
│   └── scalability_multi_system.html     # Alle 3 Systeme
└── tools/                                 # Konvertierungs-Scripts
    └── convert_to_scalability.py         # JSON-Format Converter
```

---

## 🚀 Schnellstart (3 Schritte)

### **Schritt 1: Paket entpacken**
```
Rechtsklick auf ZIP → "Alle extrahieren..."
```

### **Schritt 2: HTML öffnen**

**Option A - Multi-System Vergleich:**
```
visualizers/scalability_multi_system.html
```
→ Doppelklick → Öffnet im Browser

**Option B - Nur Flink:**
```
visualizers/flink_scalability_visualizer.html
```
→ Doppelklick → Öffnet im Browser

### **Schritt 3: Daten laden**

Im Browser:
1. **Klick "📂 Upload JSON"** (oder entsprechende Buttons)
2. **Wähle JSON aus `data/` Ordner:**
   - Flink: `scalability_flink_final.json`
   - Oracle: `scalability_oracle_final.json`
   - Trino: `scalability_trino_final.json`
3. **Fertig!** 📊

---

## 📊 Visualizer im Detail

### **1. Multi-System Vergleich** (`scalability_multi_system.html`)

**Features:**
- ✅ Vergleich von Flink, Oracle, Trino
- ✅ Zwei Ansichten: Overlay (alle in einem Chart) oder Side-by-Side
- ✅ 4 Metriken: Runtime, CPU, Memory, Throughput
- ✅ Query-Filter: Einzelne Queries isolieren
- ✅ System-Toggle: Systeme ein/ausblenden

**Upload:**
- 3 separate Upload-Felder für jedes System
- Mindestens 1 System erforderlich
- Funktioniert mit 1, 2 oder 3 Systemen

**Besonderheit:**
- Peak Memory nur bei Flink verfügbar (Info-Box wird angezeigt)

---

### **2. Flink-Only Visualizer** (`flink_scalability_visualizer.html`)

**Features:**
- ✅ Focus auf Flink's Performance
- ✅ Alle 9 Queries, 5 Dataset-Größen (1K-10M)
- ✅ 4 Metriken verfügbar
- ✅ Flink-Branding (Orange/Rot Design)

**Upload:**
- 1 Upload-Feld für `scalability_flink_final.json`

---

## 📈 Was die Daten zeigen

### **Dataset-Größen:**
- 1K = 1,000 Zeilen
- 10K = 10,000 Zeilen
- 100K = 100,000 Zeilen
- 1M = 1,000,000 Zeilen
- 10M = 10,000,000 Zeilen

### **Metriken:**

**Runtime (Elapsed Time):**
- Gesamtlaufzeit der Query
- Flink: Konstant ~4.8s über alle Größen
- Oracle: Steigt linear (1K→0.03s, 10M→75s)
- Trino: Ähnlich wie Oracle

**CPU Time:**
- Verbrauchte CPU-Zeit
- Zeigt Parallelisierungs-Effizienz

**Peak Memory:**
- Maximaler Speicherverbrauch (nur Flink)
- Konstant ~280-290 MB über alle Größen

**Throughput:**
- Rows pro Sekunde
- Flink: Steigt von 200 auf 2M rows/s
- Zeigt perfekte Skalierung

### **Queries:**
- 01_overlap_default - Overlap ohne Modifikatoren
- 02_overlap - Overlap explizit
- 03_greedy - Gieriges Matching
- 03_reluctant - Zurückhaltendes Matching
- 04_define - Mit DEFINE Klausel
- 05_ONE_ROW_PER_MATCH - Ein Ergebnis pro Match
- 06_ALL_ROWS_PER_MATCH - Alle Zeilen pro Match
- 07_SUBSET - SUBSET Klausel (Workaround bei Flink)
- 08_WITHIN - Zeitfenster (nur Flink)
- 09_Nullwerte - NULL-Handling (nur Oracle/Trino)

---

## 🔧 Eigene Benchmarks hinzufügen

### **Wenn du eigene Benchmark-Daten hast:**

1. **Konvertiere sie ins richtige Format:**
   ```bash
   python tools/convert_to_scalability.py
   ```

2. **Das Script erwartet:**
   - `results_flink.json` (Flink Format)
   - `VM_Oracle_2.json` (Oracle Format)
   - `VM_Trino.json` (Trino Format)

3. **Erstellt automatisch:**
   - `scalability_flink_final.json`
   - `scalability_oracle_final.json`
   - `scalability_trino_final.json`

---

## 📋 Systemanforderungen

**Browser:**
- Chrome 90+ (empfohlen)
- Firefox 88+
- Safari 14+
- Edge 90+

**Keine Installation erforderlich!**
- Alles läuft im Browser
- Keine Internetverbindung nötig (nach Download)
- Chart.js wird von CDN geladen

---

## 🐛 Troubleshooting

**Problem:** JSON wird nicht geladen
- **Lösung:** Prüfe ob Datei richtig benannt ist (exakt wie oben)
- Browser-Konsole öffnen (F12) für Fehlermeldungen

**Problem:** Chart ist leer
- **Lösung:** JSON-Datei könnte falsch formatiert sein
- Öffne JSON in Texteditor → muss mit `{` starten

**Problem:** Nur Flink bei Peak Memory
- **Lösung:** Das ist korrekt! Oracle/Trino haben keine Memory-Daten

**Problem:** HTMLs öffnen nicht
- **Lösung:** Rechtsklick → "Öffnen mit" → Browser wählen

---

## 📧 Support

Bei Fragen oder Problemen:
- Prüfe zuerst die Browser-Konsole (F12)
- Stelle sicher dass alle 3 JSON-Dateien im `data/` Ordner sind
- Verwende einen modernen Browser

---

## 📊 Beispiel-Screenshots

### Multi-System Overlay:
Alle 3 Systeme in einem Chart, direkter Vergleich aller Queries

### Side-by-Side:
Drei separate Charts nebeneinander, ideal für Präsentationen

### Flink-Only:
Focus auf Flink's konstante Performance-Charakteristik

---

## 🎓 Für Präsentationen

**Empfohlene Einstellungen:**

1. **Für System-Vergleich:**
   - Multi-System HTML
   - Overlay-Modus
   - Runtime Metrik
   - Query "overlap default" für klaren Vergleich

2. **Für Flink Scalability:**
   - Flink-Only HTML
   - Throughput Metrik
   - Zeigt perfekte Parallelisierung

3. **Für Memory-Analyse:**
   - Flink-Only HTML
   - Memory Metrik
   - Zeigt konstanten Speicherverbrauch

---

## ✅ Checkliste für erfolgreiche Nutzung

- [ ] ZIP entpackt
- [ ] Browser geöffnet (Chrome/Firefox/Edge)
- [ ] HTML-Datei per Doppelklick geöffnet
- [ ] JSON-Datei(en) hochgeladen
- [ ] Chart wird angezeigt
- [ ] Metriken umschaltbar
- [ ] Tooltips funktionieren (Hover über Datenpunkte)

---

**Viel Erfolg mit der Visualisierung! 🚀**
