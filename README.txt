Squid Control Center v8
=======================

Neu in v8
---------
- CSV-Export für:
  - Top URLs
  - Top Benutzer
  - Top Domains
  - Letzte Aktivitäten
  - Verhaltensmuster-Treffer
  - Trend
- HTML-Report mit:
  - Kurzreport
  - Tabellen
  - einfacher Trend-Visualisierung als Balken
- längere Auswertungszeiträume:
  - bis 43200 Minuten
  - für große Zeiträume bevorzugt SQLite-Tabelle access_events
- eigener Report-Ordner im Konfigurations-Tab
- Pfadauswahl für squid.conf, Logs und Report-Ordner
- Autosave bleibt erhalten

Wichtige Hinweise
-----------------
1. Für längere historische Auswertungen zuerst im Statistik-Tab "Access-Log importieren" ausführen.
2. Danach können längere Zeiträume aus SQLite ausgewertet werden.
3. HTML-Reports werden im konfigurierten Report-Ordner gespeichert.
4. Die Trend-Visualisierung ist bewusst einfach gehalten und ohne externe Grafikbibliotheken aufgebaut.

Start
-----
pip install PyQt5 bcrypt
python squid_control_center_v8.py


Zusätzlich in dieser vereinheitlichten v8:
- Diagramme direkt im Qt-Fenster
- Balken-Diagramme
- Kreis-Diagramme
- Python.exe Feld in der Konfiguration
