# ---------------------------------------------------------------------------
# File:   squid_manager.py
# Author: (c) 2024, 2025, 2026 Jens Kallup - paule32
# All rights reserved
# ---------------------------------------------------------------------------
from __future__  import annotations
from dataclasses import dataclass, field

import os
import sys
import re

# ---------------------------------------------------------------------------
# database module imports ....
# ---------------------------------------------------------------------------
import csv
import json
import sqlite3      # data base

# ---------------------------------------------------------------------------
# needed module imports for chm help viewer ...
# ---------------------------------------------------------------------------
import subprocess
import shutil
import hashlib
import tempfile

import traceback    # debug
import time
import ipaddress

# ---------------------------------------------------------------------------
# i18n / gettext (mo inside zip: <lang>/LC_MESSAGES/dbase.mo)
# ---------------------------------------------------------------------------
import io
import zipfile
import gettext
import polib

from pathlib         import Path
from datetime        import datetime, timedelta

from urllib.parse    import urlparse
from html            import escape
from html            import unescape
from html.parser     import HTMLParser


from PyQt5.QtCore    import (
    QObject, Qt, QTimer, qInstallMessageHandler, QEvent, QSortFilterProxyModel,
    QUrl, QPoint,
)
from PyQt5.QtGui     import (
    QStandardItemModel, QStandardItem, QPalette, QColor, QFont,
)
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QSplitter, QTabWidget, QLabel, QPushButton, QLineEdit, QMessageBox, QAction,
    QTableWidget, QTableWidgetItem, QComboBox, QTextEdit, QCheckBox, QTabBar,
    QFileDialog, QSpinBox, QHeaderView, QAbstractItemView, QSizePolicy,
    QDialog, QStyle, QTreeView, QStatusBar, QToolBar, QPlainTextEdit,
    QRadioButton, QButtonGroup, QGroupBox, QScrollArea, 
)
from PyQt5.QtWebEngineWidgets import (
    QWebEngineView, QWebEngineScript,
)

# ---------------------------------------------------------------------------
# statistics
# ---------------------------------------------------------------------------
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure                  import Figure

APP_DIR            = Path(__file__).resolve().parent
DB_PATH            = APP_DIR / "squid_manager.db"
BLOCKED_FILE       = APP_DIR / "blocked_urls.txt"
DEFAULT_SQUID_CONF = APP_DIR / "squid.conf"
DEFAULT_ACCESS_LOG = APP_DIR / "access.log"
DEFAULT_CACHE_LOG  = APP_DIR / "cache.log"

URL_CATEGORIES = [
    "FSK",
    "nicht jugendfrei",
    "Glücksspiel",
    "Betrug",
    "Scam",
    "Gewalt",
    "Sex",
    "sonstiges",
]

@dataclass
class TocNode:
    title: str
    local: Optional[str] = None
    children: List["TocNode"] = field(default_factory=list)

try:
    import bcrypt
    HAS_BCRYPT = True
except Exception:
    HAS_BCRYPT = False

# ---------------------------------------------------------------------------
# debug log file beyond the exe application ...
# ---------------------------------------------------------------------------
import faulthandler

BASE = Path(getattr(sys, "_MEIPASS", Path(sys.argv[0]).resolve().parent))
LOG  = BASE / "webengine_crash.log"

faulthandler.enable(open(LOG, "a", buffering=1), all_threads=True)

# ---------------------------------------------------------------------------
# application states for global usage ...
# ---------------------------------------------------------------------------
class AppMode_State:
    dark   = True
    lang   = "de"
    domain = "squid_manager"
    TableWidgetHeaderColor = "yellow"
    TableWidgetColor = "white"
    TableWidget_BackgroundColor = "#202020"
    TableWidget_AlternateBackgroundColor = "#333333"
# ---------------------------------------------------------------------------
AppMode = AppMode_State()

def ensure_qt_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
        app.setStyle(IconScrollBarStyle(app.style()))
    return app

# ---------------------------------------------------------------------------
# \brief display error message on exception or error ...
# ---------------------------------------------------------------------------
class ErrorMessage(QDialog):
    def __init__(self, title="Fehler", message="", log_path=None, parent=None):
        super().__init__(parent)

        self.log_path = log_path  # Pfad zur Logdatei (oder None)

        self.setWindowTitle(title)
        self.resize(750, 420)

        layout = QVBoxLayout(self)

        # Textbereich
        self.text_edit = QPlainTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setPlainText(message)
        self.text_edit.setLineWrapMode(QPlainTextEdit.NoWrap)

        font = QFont("Consolas")
        font.setStyleHint(QFont.Monospace)
        self.text_edit.setFont(font)

        layout.addWidget(self.text_edit)

        # Button-Leiste
        btn_row = QHBoxLayout()

        self.btn_delete_log = QPushButton("LOG löschen")
        self.btn_delete_log.clicked.connect(self._on_delete_log_clicked)
        self.btn_delete_log.setEnabled(bool(self.log_path))  # nur aktiv, wenn Pfad vorhanden

        btn_row.addWidget(self.btn_delete_log)
        btn_row.addStretch()

        self.btn_close = QPushButton("Schließen")
        self.btn_close.clicked.connect(self.accept)
        btn_row.addWidget(self.btn_close)

        layout.addLayout(btn_row)

    def _on_delete_log_clicked(self):
        if not self.log_path:
            return
        if not os.path.exists(self.log_path):
            QMessageBox.information(
                self,
                "LOG nicht gefunden",
                "Die LOG-Datei existiert nicht (mehr)."
            )
            return
        err = _tr("remove LOG file?")
        answer = QMessageBox.question(
            self,
            _tr("delete LOG file?"),
            f"{err}\n\n{self.log_path}",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if answer != QMessageBox.Yes:
            return
        try:
            with open(LOG, "w", encoding="utf-8"):
                pass
            os.remove(self.log_path)
        except Exception as e:
            err = _tr("LOG file could not remove")
            QMessageBox.critical(
                self,
                _tr("remove file diened."),
                f"{err}:\n{e}"
            )
            return
        QMessageBox.information(
            self,
            _tr("removed"),
            _tr("LOG file have been removed")
        )
        # Optional: Button deaktivieren, weil Datei weg ist
        self.btn_delete_log.setEnabled(False)

# ---------------------------------------------------------------------------
# Qt message handleer (for WebEngine) ...
# ---------------------------------------------------------------------------
def qt_msg_handler(mode, context, message):
    with open(LOG, "a", buffering=1) as f:
        f.write(f"[QT] {message}\n")

qInstallMessageHandler(qt_msg_handler)

# ---------------------------------------------------------------------------
# sys.argv[0] zeigt auf die gestartete EXE
# ---------------------------------------------------------------------------
def app_dir() -> Path:
    return Path(sys.argv[0]).resolve().parent
    
def load_qss(rel_path: str) -> str:
    p = app_dir() / rel_path
    return p.read_text(encoding="utf-8")

def excepthook(etype, value, tb):
    content = ""
    
    with open(LOG, "w", buffering=1) as f:
        f.write("\n--- PYTHON UNCAUGHT EXCEPTION ---\n")
        traceback.print_exception(etype, value, tb, file=f)
        f.close()
        
    app = ensure_qt_app()
    
    if app is not None:
        with open(LOG, "r") as f:
            content = f.read()
            f.close()
        
        dlg = ErrorMessage(
            title    = "Laufzeitfehler",
            message  = content,
            log_path = LOG,
            parent   = None
        )
        dlg.exec_()
    sys.__excepthook__(etype, value, tb)

sys.excepthook = excepthook
print("hook installed.")

base = Path(sys.argv[0]).resolve().parent
cand = list(base.rglob("QtWebEngineProcess.exe"))
with open("webengine_crash.log", "a", buffering=1) as f:
    f.write(f"base={base}\nQtWebEngineProcess={cand}\n")
    
try:
    def qt_msg_handler(mode, context, message):
        with open(LOG, "a", buffering=1) as f:
            f.write(f"[QT] {message}\n")
    qInstallMessageHandler(qt_msg_handler)
except Exception as e:
    print(e)
    pass

# ---------------------------------------------------------------------------
# locales (gnu gettext) support ...
# Loads GNU gettext .mo files from a zip and provides tr().
# ---------------------------------------------------------------------------
class TranslationManager:
    def __init__(self, zip_path: Optional[Union[str, Path]] = None, mode: int = 0, domain: str = "squid_manager"):
        self.domain     = domain
        self.zip_path   = Path(zip_path) if zip_path else None
        self.lang       = "de"
        self.mode       = mode
        self.style_name = "dark"
        self._trans     = gettext.NullTranslations()
    
    def set_zip(self, zip_path: Union[str, Path]):
        self.zip_path = Path(zip_path)
    
    def load_mo(self, lang: str) -> bool:
        lang            = lang.strip().lower()
        self.style_name = lang
        self.lang       = lang
        self._trans     = gettext.NullTranslations()
        
        if not self.zip_path:
            return False
        
        AppMode.lang   = lang
        AppMode.domain = self.domain
        
        if self.mode == 0:
            inner = f"locales/{lang}/LC_MESSAGES/{self.domain}.mo"
        elif self.mode == 1:
            inner = f"styles/default/{self.style_name}.mo"
        try:
            with zipfile.ZipFile(str(self.zip_path), "r") as zf:
                data = zf.read(inner)  # bytes
            self._trans = gettext.GNUTranslations(fp=io.BytesIO(data))
            return True
        except KeyError:
            # not found in zip
            self._trans = gettext.NullTranslations()
            return False
        except Exception:
            self._trans = gettext.NullTranslations()
            return False
    
    def _tr(self, msgid: str) -> str:
        try:
            return self._trans.gettext(msgid)
        except Exception:
            return msgid

# ---------------------------------------------------------------------------
# Global translation hook used by UI code: tr("File") -> "Datei" (if de loaded)
# ---------------------------------------------------------------------------
_I18N = TranslationManager( mode = 0 )
_QCSS = TranslationManager( mode = 1 )

# ---- Standard-Locale beim Start setzen ----
if os.name == "nt":
    _I18N.set_zip(Path(__file__).parent / "data\\locales.zip"); _I18N.load_mo("de"  ) # Deutsch als Default
    _QCSS.set_zip(Path(__file__).parent / "data\\styles.zip" ); _QCSS.load_mo("dark") # dark mode style
else:
    _I18N.set_zip(Path(__file__).parent / "data/locales.zip"); _I18N.load_mo("de"  ) # Deutsch als Default
    _QCSS.set_zip(Path(__file__).parent / "data/styles.zip" ); _QCSS.load_mo("dark") # dark mode style

def  _tr(msgid: str) -> str: return _I18N._tr(msgid)
def _css(msgid: str) -> str: return _QCSS._tr(msgid)


class TimeRadioPopup(QWidget):
    def __init__(self, parent_combo=None):
        super().__init__(None, Qt.Popup)
        self.parent_combo = parent_combo
        self.selected_text = ""

        self.setWindowFlags(Qt.Popup | Qt.FramelessWindowHint)
        self.setMinimumWidth(420)
        self.setMinimumHeight(320)

        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(6, 6, 6, 6)
        main_layout.setSpacing(6)

        content_layout = QHBoxLayout()
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(8)

        # Eine gemeinsame ButtonGroup => es ist immer nur EIN Radiobutton aktiv
        self.button_group = QButtonGroup(self)
        self.button_group.setExclusive(True)
        self.button_group.buttonClicked.connect(self._on_button_clicked)

        times = self._build_times()

        left_times = times[:48]   # 00:00 bis 11:45
        right_times = times[48:]  # 12:00 bis 23:45

        left_group = self._create_group("00 - 11 Uhr", left_times)
        right_group = self._create_group("12 - 23 Uhr", right_times)

        content_layout.addWidget(left_group)
        content_layout.addWidget(right_group)

        main_layout.addLayout(content_layout)

    def _build_times(self):
        result = []
        for hour in range(24):
            for minute in (0, 15, 30, 45):
                result.append(f"{hour:02d}:{minute:02d} Uhr")
        return result

    def _create_group(self, title, items):
        group_box = QGroupBox(title)
        group_layout = QVBoxLayout(group_box)
        group_layout.setContentsMargins(4, 4, 4, 4)

        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(4, 4, 4, 4)
        container_layout.setSpacing(2)

        for text in items:
            rb = QRadioButton(text)
            self.button_group.addButton(rb)
            container_layout.addWidget(rb)

        container_layout.addStretch(1)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(container)

        group_layout.addWidget(scroll)
        return group_box

    def _on_button_clicked(self, button):
        self.selected_text = button.text()

        if self.parent_combo is not None:
            self.parent_combo.set_current_text(self.selected_text)

        self.hide()

    def set_checked_text(self, text):
        for btn in self.button_group.buttons():
            if btn.text() == text:
                btn.setChecked(True)
                self.selected_text = text
                return


class TimeComboBox(QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        self.lineEdit().setPlaceholderText("Zeit auswählen ...")
        self._text = ""

        self.popup_widget = TimeRadioPopup(self)

    def showPopup(self):
        popup_pos = self.mapToGlobal(QPoint(0, self.height()))
        self.popup_widget.move(popup_pos)
        self.popup_widget.resize(max(self.width() * 2, 420), 320)
        self.popup_widget.set_checked_text(self._text)
        self.popup_widget.show()
        self.popup_widget.raise_()
        self.popup_widget.activateWindow()

    def hidePopup(self):
        self.popup_widget.hide()
        super().hidePopup()

    def set_current_text(self, text):
        self._text = text
        self.setEditText(text)

    def text(self):
        return self._text

    def currentText(self):
        return self._text


def hash_password(password: str) -> str:
    if HAS_BCRYPT:
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_domain_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        if parsed.netloc:
            return parsed.netloc.lower()
        return url.split("/")[0].lower()
    except Exception:
        return url.lower()

def parse_squid_timestamp(text: str) -> datetime:
    try:
        return datetime.fromtimestamp(float(text))
    except Exception:
        return datetime.now()

def read_tail_lines(path: Path, max_lines: int = 500):
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    return lines[-max_lines:]

def parse_access_log_line(line: str):
    parts = line.strip().split()
    if len(parts) < 7:
        return None

    ts          = parts[0]
    elapsed     = parts[1] if len(parts) > 1 else ""
    client_ip   = parts[2] if len(parts) > 2 else ""
    result_code = parts[3] if len(parts) > 3 else ""
    byte_count  = parts[4] if len(parts) > 4 else ""
    method      = parts[5] if len(parts) > 5 else ""
    url         = parts[6] if len(parts) > 6 else ""
    username    = parts[7] if len(parts) > 7 else "-"

    return {
        "timestamp"  : parse_squid_timestamp(ts),
        "elapsed_ms" : elapsed,
        "client_ip"  : client_ip,
        "result_code": result_code,
        "bytes"      : int(byte_count) if str(byte_count).isdigit() else 0,
        "method"     : method,
        "url"        : url,
        "domain"     : safe_domain_from_url(url),
        "username"   : username,
        "raw_line"   : line.rstrip("\n"),
    }

def item_text(table: QTableWidget, row: int, col: int) -> str:
    it = table.item(row, col)
    return it.text() if it else ""

def html_table(headers, rows):
    out = ["<table border='1' cellspacing='0' cellpadding='4'>", "<tr>"]
    for h in headers:
        out.append(f"<th>{escape(str(h))}</th>")
    out.append("</tr>")
    for row in rows:
        out.append("<tr>")
        for cell in row:
            out.append(f"<td>{escape(str(cell))}</td>")
        out.append("</tr>")
    out.append("</table>")
    return "".join(out)

class HtmlHelpParser(HTMLParser):
    """
    Parser für htmlhelp .hhc (Contents) und .hhk (Index).
    Beide nutzen:
      <OBJECT type="text/sitemap">
         <param name="Name" value="...">
         <param name="Local" value="...">
      </OBJECT>
      <UL> ... </UL> (optional)
    """
    def __init__(self):
        super().__init__()
        self.root = TocNode("ROOT")
        self._stack: List[TocNode] = [self.root]

        self._in_object = False
        self._cur_name: Optional[str] = None
        self._cur_local: Optional[str] = None

        self._last_created: Optional[TocNode] = None
        self._pending_push_on_ul = False

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs = {k.lower(): v for k, v in attrs}

        if tag == "object":
            t = (attrs.get("type") or "").lower()
            if "text/sitemap" in t:
                self._in_object = True
                self._cur_name = None
                self._cur_local = None

        elif tag == "param" and self._in_object:
            name = (attrs.get("name") or "").lower()
            value = (attrs.get("value") or "").strip()
            if name == "name":
                self._cur_name = value
            elif name == "local":
                self._cur_local = value

        elif tag == "ul":
            if self._pending_push_on_ul and self._last_created is not None:
                self._stack.append(self._last_created)
                self._pending_push_on_ul = False

    def handle_endtag(self, tag):
        tag = tag.lower()

        if tag == "object" and self._in_object:
            self._in_object = False
            title = (self._cur_name or "Untitled").strip()
            local = (self._cur_local or "").strip() or None

            node = TocNode(title=title, local=local)
            self._stack[-1].children.append(node)

            self._last_created = node
            self._pending_push_on_ul = True

        elif tag == "ul":
            if len(self._stack) > 1:
                self._stack.pop()

    def unknown_decl(self, data):
        pass

def _read_text_fallback(path: str) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read()
        except Exception:
            pass
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

def parse_hh_file(path: str) -> TocNode:
    raw = _read_text_fallback(path)
    p = HtmlHelpParser()
    p.feed(raw)
    return p.root

class RecursiveFilterProxy(QSortFilterProxyModel):
    def __init__(self):
        super().__init__()
        self._text = ""
        self.setFilterCaseSensitivity(Qt.CaseInsensitive)
        if hasattr(self, "setRecursiveFilteringEnabled"):
            self.setRecursiveFilteringEnabled(True)

    def setFilterText(self, text: str):
        self._text = (text or "").strip()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        if not self._text:
            return True

        model = self.sourceModel()
        idx = model.index(source_row, 0, source_parent)
        if not idx.isValid():
            return False

        title = model.data(idx, Qt.DisplayRole) or ""
        if self._text.lower() in title.lower():
            return True

        for r in range(model.rowCount(idx)):
            if self.filterAcceptsRow(r, idx):
                return True
        return False

def decompile_chm_windows(chm_path: str, out_dir: str) -> bool:
    """
    Windows-only: uses hh.exe -decompile OUTDIR file.chm
    """
    hh = shutil.which("hh.exe") or shutil.which("hh")
    if not hh:
        print("hh.exe not found !")
        return False
    try:
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= startupinfo.STARTF_USESHOWWINDOW
        
            p = subprocess.Popen(
                [hh, "-decompile", out_dir, chm_path],
                creationflags  = subprocess.CREATE_NO_WINDOW,
                startupinfo    = startupinfo,
                capture_output = True,
                text           = True,
                stdout         = subprocess.PIPE,
                stderr         = subprocess.PIPE
            )
            out, err = p.communicate(timeout=60)
            if p.returncode != 0:
                raise RuntimeError(f"hh.exe failed ({p.returncode}):\n{err}")
            return True
        else:
            content = "CHM Help is only available under Microsoft Windows"
            dlg = ErrorMessage(
                title    = "Laufzeitfehler",
                message  = content,
                log_path = LOG,
                parent   = MAINWIN
            )
            dlg.exec_()
            return False
    except Exception as e:
        print(e)
        return False

class HelpMainWindow(QMainWindow):
    ROLE_LOCAL = Qt.UserRole + 1
    ROLE_BREAD = Qt.UserRole + 2

    def __init__(self):
        super().__init__()
        
        self._pending_page: Optional[str] = None
        
        self._resize_margin = 8  # Pixel "Griffbreite" am Rand
        self._resizing      = False
        self._resize_edge   = None
        self._drag_pos      = None
        self._start_geom    = None
        
        self.setWindowTitle("CHM-Viewer - (c) 2026 Jens Kallup - paule32")
        self.resize(800, 600)
        
        #self.setWindowFlags(self.windowFlags() | Qt.FramelessWindowHint)
        
        top = QWidget()
        top.setObjectName("TopContainer")
        top_lay = QVBoxLayout(top)
        top_lay.setContentsMargins(0, 0, 0, 0)
        top_lay.setSpacing(0)

        # Optional: dünne Trennlinie unter der Titelleiste
        sep = QWidget()
        sep.setObjectName("TitleSeparator")
        sep.setFixedHeight(1)
        top_lay.addWidget(sep)

        self.setMenuWidget(top)
        
        self.base_dir: Optional[str] = None
        AppMode.dark = True

        # Icons
        try:
            self.icon_book = icon_from_svg(SVG_BOOK, 16)
            self.icon_page = icon_from_svg(SVG_PAGE, 16)
        except Exception:
            self.icon_book = self.style().standardIcon(QStyle.SP_DirIcon)
            self.icon_page = self.style().standardIcon(QStyle.SP_FileIcon)

        # Web
        self.web = QWebEngineView()
        self.web.urlChanged.connect(self._on_url_changed)

        # Tabs left
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        self.tabs.setUsesScrollButtons(True)
        self.tabs.tabBar().setUsesScrollButtons(True)

        # Contents model/view
        self.contents_model = QStandardItemModel()
        self.contents_model.setHorizontalHeaderLabels(["Contents"])
        self.contents_proxy = RecursiveFilterProxy()
        self.contents_proxy.setSourceModel(self.contents_model)

        self.contents_filter = QLineEdit()
        self.contents_filter.setPlaceholderText("Filter (Contents)…")
        self.contents_filter.textChanged.connect(self.contents_proxy.setFilterText)

        self.contents_tree = QTreeView()
        self.contents_tree.setModel(self.contents_proxy)
        self.contents_tree.setUniformRowHeights(True)
        self.contents_tree.clicked.connect(self.on_contents_clicked)

        tab_contents = QWidget()
        vc = QVBoxLayout(tab_contents)
        vc.setContentsMargins(8, 8, 8, 8)
        vc.setSpacing(8)
        vc.addWidget(self.contents_filter)
        vc.addWidget(self.contents_tree)
        self.tabs.addTab(tab_contents, "Contents")

        # Index model/view
        self.index_model = QStandardItemModel()
        self.index_model.setHorizontalHeaderLabels(["Index"])
        self.index_proxy = RecursiveFilterProxy()
        self.index_proxy.setSourceModel(self.index_model)

        self.index_filter = QLineEdit()
        self.index_filter.setPlaceholderText("Filter (Index)…")
        self.index_filter.textChanged.connect(self.index_proxy.setFilterText)

        self.index_view = QTreeView()
        self.index_view.setModel(self.index_proxy)
        self.index_view.setUniformRowHeights(True)
        self.index_view.clicked.connect(self.on_index_clicked)

        tab_index = QWidget()
        vi = QVBoxLayout(tab_index)
        vi.setContentsMargins(8, 8, 8, 8)
        vi.setSpacing(8)
        vi.addWidget(self.index_filter)
        vi.addWidget(self.index_view)
        self.tabs.addTab(tab_index, "Index")

        # Search tab (Sphinx search.html)
        tab_search = QWidget()
        vs = QVBoxLayout(tab_search)
        vs.setContentsMargins(8, 8, 8, 8)
        vs.setSpacing(8)

        row = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search (Sphinx)…")
        self.search_edit.returnPressed.connect(self.open_sphinx_search)
        btn = QPushButton("Search")
        btn.clicked.connect(self.open_sphinx_search)
        row.addWidget(self.search_edit, 1)
        row.addWidget(btn, 0)

        hint = QLabel("Sucht in Sphinx über search.html – Ergebnisse erscheinen rechts.")
        hint.setWordWrap(True)
        hint.setStyleSheet("opacity: 0.8;")

        vs.addLayout(row)
        vs.addWidget(hint)
        vs.addStretch(1)
        self.tabs.addTab(tab_search, "Search")

        # Splitter
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self.tabs)
        splitter.addWidget(self.web)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([380, 820])

        self.status = QStatusBar(self)
        self.setStatusBar(self.status)
        self.status.showMessage("Ready", 2000)
        
        central = QWidget()
        lay = QVBoxLayout(central)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addWidget(splitter)
        self.setCentralWidget(central)
        
        self._make_toolbar()
        #self._apply_theme()

    # -------- Toolbar --------
    def _make_toolbar(self):
        tb = QToolBar("Main")
        tb.setMovable(False)
        self.addToolBar(tb)

        act_open = QAction(self.style().standardIcon(QStyle.SP_DialogOpenButton), "Open…", self)
        act_open.triggered.connect(self.open_chm_single_dialog)
        tb.addAction(act_open)

        tb.addSeparator()

        act_home = QAction(self.style().standardIcon(QStyle.SP_ArrowUp), "Home", self)
        act_home.triggered.connect(self.go_home)
        tb.addAction(act_home)

        act_back = QAction(self.style().standardIcon(QStyle.SP_ArrowBack), "Back", self)
        act_back.triggered.connect(self.web.back)
        tb.addAction(act_back)

        act_fwd = QAction(self.style().standardIcon(QStyle.SP_ArrowForward), "Forward", self)
        act_fwd.triggered.connect(self.web.forward)
        tb.addAction(act_fwd)

        act_reload = QAction(self.style().standardIcon(QStyle.SP_BrowserReload), "Reload", self)
        act_reload.triggered.connect(self.web.reload)
        tb.addAction(act_reload)

        tb.addSeparator()

        self.breadcrumb = QLabel("—")
        self.breadcrumb.setTextInteractionFlags(Qt.TextSelectableByMouse)
        tb.addWidget(self.breadcrumb)

        tb.addSeparator()

        self.act_theme = QAction("🌙 Dark", self)
        self.act_theme.triggered.connect(self.toggle_theme)
        tb.addAction(self.act_theme)

    # -------- Open (single dialog) --------
    def open_chm_single_dialog(self):
        chm_path, _ = QFileDialog.getOpenFileName(
            self,
            "Open CHM",
            "",
            "CHM Help (*.chm);;All Files (*)"
        )
        if not chm_path:
            return

        self.load_from_chm_path(chm_path)

    # -------- Load from CHM path --------
    def load_from_chm_path(self, chm_path: str):
        """
        1) Try: side-by-side .hhc/.hhk in same folder as CHM
        2) Else: Windows hh.exe -decompile to temp -> load .hhc/.hhk from there
        """
        folder = os.path.dirname(chm_path)
        stem = os.path.splitext(os.path.basename(chm_path))[0]

        hhc = os.path.join(folder, f"{stem}.hhc")
        hhk = os.path.join(folder, f"{stem}.hhk")

        if os.path.exists(hhc):
            self.base_dir = folder
            print(self.base_dir)
            self.load_contents(hhc)
            if os.path.exists(hhk):
                self.load_index(hhk)
            else:
                self.index_model.removeRows(0, self.index_model.rowCount())
            self.open_start_page()
            return

        # fallback: decompile CHM
        tmp = tempfile.mkdtemp(prefix="chm_decompile_")
        ok = decompile_chm_windows(chm_path, tmp)

        if not ok:
            QMessageBox.warning(
                self,
                "TOC nicht verfügbar",
                "Keine passende .hhc neben der CHM gefunden und CHM konnte nicht dekompiliert werden.\n\n"
                "Windows: stelle sicher, dass 'hh.exe' verfügbar ist.\n"
                "Alternative: CHM manuell dekompilieren und dann die entpackten Dateien anzeigen."
            )
            return

        # pick first .hhc/.hhk in temp
        hhc_found = self._find_first(tmp, (".hhc",))
        hhk_found = self._find_first(tmp, (".hhk",))

        if not hhc_found:
            QMessageBox.warning(self, "Keine .hhc gefunden", "Nach Dekomplilierung wurde keine .hhc gefunden.")
            return

        self.base_dir = tmp
        self.load_contents(hhc_found)
        
        if hhk_found:
            self.load_index(hhk_found)
        else:
            self.index_model.removeRows(0, self.index_model.rowCount())

        self.open_start_page()
        
    def open_from_args(self, chm_path: Optional[str], page: Optional[str]):
        """
        Wird einmal beim Start aufgerufen.
        - chm_path: Pfad zur .chm
        - page: relative Seite, z.B. "index.html" oder "api/mod.html#func"
        """
        if page:
            self._pending_page = page

        if chm_path:
            chm_path = str(app_dir()) + "\\data\\" + chm_path
            chm_path = chm_path.replace("/", "\\")

            self.load_from_chm_path(chm_path)

            # nach dem Laden ggf. die Seite öffnen
            if self._pending_page:
                self.open_local(self._pending_page)
                self._pending_page = None
                
    def open_start_page(self):
        if not self.base_dir:
            return
        index_html = os.path.join(self.base_dir, "index.html")
        if os.path.exists(index_html):
            self.web.setUrl(QUrl.fromLocalFile(index_html))
        else:
            first = self._first_local_item(self.contents_model)
            if first:
                self.open_local(first)

    # -------- Contents / Index load --------
    def load_contents(self, hhc_path: str):
        self.contents_model.removeRows(0, self.contents_model.rowCount())
        toc_root = parse_hh_file(hhc_path)
        for child in toc_root.children:
            self.contents_model.appendRow(self._node_to_item(child, parent_path=[]))
        self.contents_tree.expandToDepth(1)

    def load_index(self, hhk_path: str):
        self.index_model.removeRows(0, self.index_model.rowCount())
        idx_root = parse_hh_file(hhk_path)

        # flatten index entries
        items: List[Tuple[str, str]] = []

        def walk(n: TocNode):
            if n.local:
                items.append((n.title.strip(), n.local.strip()))
            for c in n.children:
                walk(c)

        for c in idx_root.children:
            walk(c)

        # Dedup:
        # 1) bevorzugt nach Local (Ziel) deduplizieren
        # 2) falls Local leer/komisch wäre: nach (title, local)
        seen_local = set()
        seen_pair = set()
        deduped: List[Tuple[str, str]] = []

        for title, local in items:
            key_local = (local or "").lower()
            key_pair = (title.lower(), key_local)

            if key_local:
                if key_local in seen_local:
                    continue
                seen_local.add(key_local)
            else:
                if key_pair in seen_pair:
                    continue
                seen_pair.add(key_pair)

            deduped.append((title, local))

        # sort by title
        deduped.sort(key=lambda x: x[0].lower())

        for title, local in deduped:
            it = QStandardItem(title)
            it.setEditable(False)
            it.setIcon(self.icon_page)
            it.setData(local, self.ROLE_LOCAL)
            it.setData(title, self.ROLE_BREAD)
            self.index_model.appendRow(it)


    def _node_to_item(self, node: TocNode, parent_path: List[str]) -> QStandardItem:
        item = QStandardItem(node.title)
        item.setEditable(False)
        item.setIcon(self.icon_book if node.children else self.icon_page)

        bread = " › ".join(parent_path + [node.title])
        item.setData(node.local or "", self.ROLE_LOCAL)
        item.setData(bread, self.ROLE_BREAD)

        for c in node.children:
            item.appendRow(self._node_to_item(c, parent_path + [node.title]))
        return item

    def _find_first(self, folder: str, exts: Tuple[str, ...]) -> Optional[str]:
        for fn in os.listdir(folder):
            if fn.lower().endswith(exts):
                fo = os.path.join(folder, fn)
                return fo
        return None

    def _first_local_item(self, model: QStandardItemModel) -> Optional[str]:
        def walk(it: QStandardItem) -> Optional[str]:
            loc = it.data(self.ROLE_LOCAL)
            if loc:
                return loc
            for r in range(it.rowCount()):
                v = walk(it.child(r))
                if v:
                    return v
            return None

        for r in range(model.rowCount()):
            v = walk(model.item(r))
            if v:
                return v
        return None

    # -------- Click handlers --------
    def on_contents_clicked(self, proxy_idx: QModelIndex):
        src_idx = self.contents_proxy.mapToSource(proxy_idx)
        item = self.contents_model.itemFromIndex(src_idx)
        if item:
            self._open_item(item)

    def on_index_clicked(self, proxy_idx: QModelIndex):
        src_idx = self.index_proxy.mapToSource(proxy_idx)
        item = self.index_model.itemFromIndex(src_idx)
        if item:
            self._open_item(item)

    def _open_item(self, item: QStandardItem):
        local = (item.data(self.ROLE_LOCAL) or "").strip()
        bread = (item.data(self.ROLE_BREAD) or "—").strip()
        self.breadcrumb.setText(bread)
        if local:
            self.open_local(local)

    def _hit_test_edge(self, pos):
        """
        Ermittelt, ob Maus in Resize-Zone ist.
        Rückgabe: string aus {L,R,T,B,LT,RT,LB,RB} oder None
        """
        m = self._resize_margin
        x, y = pos.x(), pos.y()
        w, h = self.width(), self.height()

        left = x <= m
        right = x >= w - m
        top = y <= m
        bottom = y >= h - m

        if top and left:
            return "LT"
        if top and right:
            return "RT"
        if bottom and left:
            return "LB"
        if bottom and right:
            return "RB"
        if left:
            return "L"
        if right:
            return "R"
        if top:
            return "T"
        if bottom:
            return "B"
        return None


    def _set_cursor_for_edge(self, edge):
        pass
        #"""if edge in ("L", "R"):
        #    self.setCursor(Qt.SizeHorCursor)
        #elif edge in ("T", "B"):
        #    self.setCursor(Qt.SizeVerCursor)
        #elif edge in ("LT", "RB"):
        #    self.setCursor(Qt.SizeFDiagCursor)
        #elif edge in ("RT", "LB"):
        #    self.setCursor(Qt.SizeBDiagCursor)
        #else:
        #    self.setCursor(Qt.ArrowCursor)"""


    def mouseMoveEvent(self, event):
        if self.isMaximized():
            self._set_cursor_for_edge(None)
            return super().mouseMoveEvent(event)

        if self._resizing and self._resize_edge and self._start_geom and self._drag_pos:
            delta = event.globalPos() - self._drag_pos
            g = QRect(self._start_geom)

            min_w, min_h = 400, 300  # Mindestgröße, anpassen wenn du willst

            if "L" in self._resize_edge:
                new_left = g.left() + delta.x()
                if g.right() - new_left + 1 >= min_w:
                    g.setLeft(new_left)
            if "R" in self._resize_edge:
                new_right = g.right() + delta.x()
                if new_right - g.left() + 1 >= min_w:
                    g.setRight(new_right)
            if "T" in self._resize_edge:
                new_top = g.top() + delta.y()
                if g.bottom() - new_top + 1 >= min_h:
                    g.setTop(new_top)
            if "B" in self._resize_edge:
                new_bottom = g.bottom() + delta.y()
                if new_bottom - g.top() + 1 >= min_h:
                    g.setBottom(new_bottom)

            self.setGeometry(g)
            return

        # nicht resizing: nur Cursor setzen
        edge = self._hit_test_edge(event.pos())
        self._set_cursor_for_edge(edge)
        super().mouseMoveEvent(event)


    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton and not self.isMaximized():
            edge = self._hit_test_edge(event.pos())
            if edge:
                self._resizing = True
                self._resize_edge = edge
                self._drag_pos = event.globalPos()
                self._start_geom = self.geometry()
                event.accept()
                return
        super().mousePressEvent(event)


    def mouseReleaseEvent(self, event):
        self._resizing = False
        self._resize_edge = None
        self._drag_pos = None
        self._start_geom = None
        super().mouseReleaseEvent(event)

    # -------- Robust open_local --------
    def open_local(self, local: str):
        if not self.base_dir:
            return

        local = unescape((local or "").strip())
        if not local:
            return

        # already URL?
        if re.match(r"^[a-zA-Z]+://", local):
            self.web.setUrl(QUrl(local))
            return

        # split fragment
        path_part, frag = (local.split("#", 1) + [""])[:2]
        path_part = path_part.replace("\\", "/").lstrip("/")

        abs_path = os.path.normpath(os.path.join(self.base_dir, path_part))

        # safety: stay inside base_dir
        base_norm = os.path.normpath(self.base_dir)
        if not os.path.normpath(abs_path).startswith(base_norm):
            QMessageBox.warning(self, "Ungültiger Pfad", f"Pfad außerhalb Basisordner:\n{abs_path}")
            return

        if not os.path.exists(abs_path):
            QMessageBox.warning(self, "Nicht gefunden", f"Datei nicht gefunden:\n{abs_path}")
            return

        url = QUrl.fromLocalFile(abs_path)
        if frag:
            url.setFragment(frag)
        self.web.setUrl(url)

    # -------- Search (Sphinx) --------
    def open_sphinx_search(self):
        if not self.base_dir:
            return
        q = (self.search_edit.text() or "").strip()
        if not q:
            return

        search_html = os.path.join(self.base_dir, "search.html")
        if not os.path.exists(search_html):
            QMessageBox.warning(self, "search.html fehlt", "Im htmlhelp-Ordner gibt es keine search.html.")
            return

        url = QUrl.fromLocalFile(search_html)
        q_enc = re.sub(r"\s+", "+", q)
        url.setQuery(f"q={q_enc}")
        self.web.setUrl(url)

    # -------- Navigation --------
    def go_home(self):
        if not self.base_dir:
            return
        home = os.path.join(self.base_dir, "index.html")
        if os.path.exists(home):
            self.web.setUrl(QUrl.fromLocalFile(home))

    # -------- Theme --------
    def toggle_theme(self):
        AppMode.dark = not AppMode.dark
        self.act_theme.setText("☀️ Light" if AppMode.dark else "🌙 Dark")
        self._apply_theme()
        self._inject_web_css()

    def _apply_webview_theme(self):
        """
        Injiziert CSS + toggelt .dark auf <html>.
        Call bei Theme-Wechsel UND idealerweise nach jeder Navigation.
        """
        view = self.web  # <- passe an deinen Namen an (QWebEngineView)
        css = self._webview_scrollbar_css()

        # Wichtig: CSS in JS sicher einbetten
        css_js = css.replace("\\", "\\\\").replace("`", "\\`")

        js = f"""
(function() {{
const STYLE_ID = "win95-scrollbars-style";

// dark togglen auf <html>
const root = document.documentElement;
root.classList.toggle("dark", {str(bool(AppMode.dark)).lower()});

// Style-Tag erstellen/ersetzen
let tag = document.getElementById(STYLE_ID);
if (!tag) {{
tag = document.createElement("style");
tag.id = STYLE_ID;
document.head.appendChild(tag);
}}
tag.textContent = `{css_js}`;
}})();"""

        # 1) sofort auf aktueller Seite anwenden
        view.page().runJavaScript(js)

        # 2) zusätzlich als QWebEngineScript setzen, damit es bei Navigation automatisch wirkt
        script = QWebEngineScript()
        script.setName("win95-scrollbars")
        script.setInjectionPoint(QWebEngineScript.DocumentReady)
        script.setWorldId(QWebEngineScript.MainWorld)
        script.setRunsOnSubFrames(True)  # auch iframes
        script.setSourceCode(js)

        # vorhandenes Script gleichen Namens entfernen (sonst stapelt es)
        scripts = view.page().scripts()
        for s in scripts.toList():
            if s.name() == "win95-scrollbars":
                scripts.remove(s)
                break
        scripts.insert(script)
        self._apply_webview_theme()

    def _webview_scrollbar_css(self) -> str:
        # Wir toggeln im HTML einfach "dark" auf <html> (document.documentElement)
        return r"""
/* === Win95 Scrollbars nur im rechten WebView-Dokument === */

/* Default (Light) */
:root {
  --sb-size: 16px;

  --sb-face:  #c0c0c0;
  --sb-track: #e6e6e6;
  --sb-thumb: #c0c0c0;
  --sb-hi:    #ffffff;
  --sb-mid:   #808080;
  --sb-dark:  #000000;
}

/* Dark Mode: Win95-Stil, aber navy (Balken) */
:root.dark {
  --sb-face:  #001f4d;   /* navy */
  --sb-track: #001a40;   /* etwas dunkler */
  --sb-thumb: #002b66;   /* thumb leicht heller */
  --sb-hi:    #2d5aa0;   /* “highlight” blau */
  --sb-mid:   #000b1a;   /* tiefer schatten */
  --sb-dark:  #000000;
}

/* Grundform */
::-webkit-scrollbar {
  width: var(--sb-size);
  height: var(--sb-size);
  background: var(--sb-face);
}

::-webkit-scrollbar-track {
  background: var(--sb-track);
  box-shadow:
    inset 1px 1px 0 var(--sb-mid),
    inset -1px -1px 0 var(--sb-hi);
  border: 1px solid var(--sb-dark);
}

::-webkit-scrollbar-thumb {
  background: var(--sb-thumb);
  border-top: 1px solid var(--sb-hi);
  border-left: 1px solid var(--sb-hi);
  border-right: 1px solid var(--sb-mid);
  border-bottom: 1px solid var(--sb-mid);
  outline: 1px solid var(--sb-dark);
}

/* Ecke */
::-webkit-scrollbar-corner {
  background: var(--sb-face);
  border-top: 1px solid var(--sb-mid);
  border-left: 1px solid var(--sb-mid);
}

/* Buttons */
::-webkit-scrollbar-button {
  width: var(--sb-size);
  height: var(--sb-size);
  background: var(--sb-face);
  border-top: 1px solid var(--sb-hi);
  border-left: 1px solid var(--sb-hi);
  border-right: 1px solid var(--sb-mid);
  border-bottom: 1px solid var(--sb-mid);
  outline: 1px solid var(--sb-dark);

  background-repeat: no-repeat;
  background-position: center;
  background-size: 10px 10px;
}

::-webkit-scrollbar-button:active {
  border-top: 1px solid var(--sb-mid);
  border-left: 1px solid var(--sb-mid);
  border-right: 1px solid var(--sb-hi);
  border-bottom: 1px solid var(--sb-hi);
}

/* ===== Pfeile LIGHT: schwarz ===== */
html:not(.dark) ::-webkit-scrollbar-button:single-button:vertical:decrement { background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23000' d='M5 2 L9 7 H1 Z'/></svg>") !important;}
html:not(.dark) ::-webkit-scrollbar-button:single-button:vertical:increment { background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23000' d='M1 3 H9 L5 8 Z'/></svg>") !important;}
html:not(.dark) ::-webkit-scrollbar-button:single-button:horizontal:decrement {background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23000' d='M2 5 L7 1 V9 Z'/></svg>") !important;}
html:not(.dark) ::-webkit-scrollbar-button:single-button:horizontal:increment {background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23000' d='M8 5 L3 1 V9 Z'/></svg>") !important;}

/* ===== Pfeile DARK: gelb ===== */
html.dark ::-webkit-scrollbar-button:single-button:vertical:decrement { background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23FFD400' d='M5 2 L9 7 H1 Z'/></svg>") !important;}
html.dark ::-webkit-scrollbar-button:single-button:vertical:increment { background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23FFD400' d='M1 3 H9 L5 8 Z'/></svg>") !important;}
html.dark ::-webkit-scrollbar-button:single-button:horizontal:decrement {background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23FFD400' d='M2 5 L7 1 V9 Z'/></svg>") !important;}
html.dark ::-webkit-scrollbar-button:single-button:horizontal:increment {background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='10' viewBox='0 0 10 10'><path fill='%23FFD400' d='M8 5 L3 1 V9 Z'/></svg>") !important;}
"""

    def _inject_web_css(self):
        if AppMode.dark:
            js = """
(function(){const id='__qt_dark_css__';let s=document.getElementById(id);if(!s){s=document.createElement('style');
s.id=id;s.innerHTML=`html, body { background-color:#040404 !important;color:#eaeaea !important;}
a { color:#8ab4ff !important;}pre, code { background:#1e1e1e !important;}
`;document.head.appendChild(s);}})();"""
        else:
            js = """(function(){const s=document.getElementById('__qt_dark_css__');if(s) s.remove();})();"""
        self.web.page().runJavaScript(js)

    def _on_url_changed(self, url: QUrl):
        self._inject_web_css()
        
    def _style_theme_button(self):
        # sorgt dafür, dass der Button im Dark Mode wirklich "dark" aussieht
        # (QAction selbst ist kein Widget, aber wir können die Toolbar/Button-Styles über QSS steuern)
        pass

class F1Filter(QObject):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._help_sub  = None  # optional: merken, damit wir nicht 100 Fenster öffnen
        
    def eventFilter(self, obj, event):
        if event.type() == QEvent.KeyPress and event.key() == Qt.Key_F1:
            print("F1 global abgefangen")
            # optional: wenn schon offen, nur nach vorne holen
            if self._help_sub is not None and not self._help_sub.isHidden():
                self._help_sub.showNormal()
                self._help_sub.raise_()
                return True
            
            self._help_sub = HelpMainWindow()
            self._help_sub.show()
            AppMode.dark = True
            return True
        return super().eventFilter(obj, event)

class DB:
    @staticmethod
    def conn():
        return sqlite3.connect(DB_PATH)

    @staticmethod
    def fetchall(sql: str, params=()):
        conn = DB.conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        conn.close()
        return rows

    @staticmethod
    def execute(sql: str, params=()):
        conn = DB.conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        lastrowid = cur.lastrowid
        conn.close()
        return lastrowid

    @staticmethod
    def setting(key: str, default: str = "") -> str:
        rows = DB.fetchall("SELECT value FROM settings WHERE key=?", (key,))
        return rows[0][0] if rows else default

    @staticmethod
    def set_setting(key: str, value: str):
        conn = DB.conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()
        conn.close()


def init_db():
    conn = DB.conn()
    c = conn.cursor()

    c.execute("""
CREATE TABLE IF NOT EXISTS settings (
key TEXT PRIMARY KEY,
value TEXT
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS groups (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT UNIQUE NOT NULL,
is_enabled INTEGER NOT NULL DEFAULT 1,
comment TEXT DEFAULT ''
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS networks (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT UNIQUE NOT NULL,
cidr TEXT NOT NULL,
is_enabled INTEGER NOT NULL DEFAULT 1,
comment TEXT DEFAULT ''
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS time_windows (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT UNIQUE NOT NULL,
weekdays TEXT DEFAULT '',
start_time TEXT DEFAULT '',
end_time TEXT DEFAULT '',
is_enabled INTEGER NOT NULL DEFAULT 1,
comment TEXT DEFAULT ''
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS users (
id INTEGER PRIMARY KEY AUTOINCREMENT,
username TEXT UNIQUE NOT NULL,
password_hash TEXT DEFAULT '',
cert_fingerprint TEXT DEFAULT '',
is_enabled INTEGER NOT NULL DEFAULT 1,
is_blocked INTEGER NOT NULL DEFAULT 0,
group_id INTEGER,
network_id INTEGER,
time_window_id INTEGER,
created_at TEXT DEFAULT CURRENT_TIMESTAMP
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS replacement_pages (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT UNIQUE NOT NULL,
category TEXT DEFAULT '',
file_path TEXT NOT NULL,
is_enabled INTEGER NOT NULL DEFAULT 1,
comment TEXT DEFAULT ''
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS blocked_urls (
id INTEGER PRIMARY KEY AUTOINCREMENT,
pattern TEXT NOT NULL,
category TEXT DEFAULT 'sonstiges',
is_regex INTEGER NOT NULL DEFAULT 0,
is_enabled INTEGER NOT NULL DEFAULT 1,
replacement_page_id INTEGER,
comment TEXT DEFAULT '',
created_at TEXT DEFAULT CURRENT_TIMESTAMP
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS behavior_rules (
id INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT UNIQUE NOT NULL,
url_pattern TEXT NOT NULL,
category TEXT DEFAULT '',
is_regex INTEGER NOT NULL DEFAULT 0,
scope_type TEXT DEFAULT 'all',
scope_value TEXT DEFAULT '',
window_minutes INTEGER NOT NULL DEFAULT 60,
threshold_count INTEGER NOT NULL DEFAULT 10,
is_enabled INTEGER NOT NULL DEFAULT 1,
comment TEXT DEFAULT ''
)""")

    c.execute("""
CREATE TABLE IF NOT EXISTS access_events (
id INTEGER PRIMARY KEY AUTOINCREMENT,
ts TEXT,
username TEXT,
client_ip TEXT,
method TEXT,
url TEXT,
domain TEXT,
result_code TEXT,
bytes INTEGER DEFAULT 0
)""")

    defaults = {
        "access_log_path": str(DEFAULT_ACCESS_LOG),
        "cache_log_path": str(DEFAULT_CACHE_LOG),
        "squid_service_name": "Squid",
        "squid_binary": "squid",
        "squid_conf_path": str(DEFAULT_SQUID_CONF),
        "python_exe_path": "C:/Python311/python.exe",
        "autosave_minutes": "15",
        "last_autosave": "",
        "report_output_dir": str(APP_DIR / "reports"),
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, v))

    conn.commit()
    conn.close()


def table_to_rows(table: QTableWidget):
    headers = [table.horizontalHeaderItem(i).text() if table.horizontalHeaderItem(i) else f"col{i}" for i in range(table.columnCount())]
    rows = []
    for r in range(table.rowCount()):
        row = []
        for c in range(table.columnCount()):
            row.append(item_text(table, r, c))
        rows.append(row)
    return headers, rows


class LedLabel(QLabel):
    def __init__(self):
        super().__init__()
        self.setFixedSize(18, 18)
        self.set_red()

    def set_red(self):
        self.setStyleSheet("background:#d12f2f;border-radius:9px;border:1px solid #555;")

    def set_green(self):
        self.setStyleSheet("background:#2dbd4f;border-radius:9px;border:1px solid #555;")

    def set_yellow(self):
        self.setStyleSheet("background:#d5b12e;border-radius:9px;border:1px solid #555;")


class BaseCrudTab(QWidget):
    def message(self, title, text):
        QMessageBox.information(self, title, text)

    def warn(self, title, text):
        QMessageBox.warning(self, title, text)

    def fill_table(self, table, headers, rows):
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                table.setItem(r, c, QTableWidgetItem("" if val is None else str(val)))
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)


class MplCanvas(FigureCanvas):
    def __init__(self):
        self.figure = Figure(figsize=(7, 4), tight_layout=True)
        self.ax = self.figure.add_subplot(111)
        super().__init__(self.figure)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def clear(self):
        self.figure.clear()
        self.ax = self.figure.add_subplot(111)


class DashboardTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        top = QHBoxLayout()

        self.service_led = LedLabel()
        self.activity_led = LedLabel()
        self.activity_led.set_yellow()

        top.addWidget(QLabel("Squid Dienst"))
        top.addWidget(self.service_led)
        top.addSpacing(20)
        top.addWidget(QLabel("Proxy Aktivität"))
        top.addWidget(self.activity_led)
        top.addStretch(1)
        
        btnhlay = QHBoxLayout()
        
        for label, handler in [
            ("Start", self.start_service),
            ("Stop", self.stop_service),
            ("Neu starten", self.restart_service),
            ("Reload", self.reload_service),
            ("Konfiguration testen", self.test_config),
            ("Aktualisieren", self.refresh),
        ]:
            b = QPushButton(label)
            b.clicked.connect(handler)
            btnhlay.addWidget(b)
        
        
        root.addLayout(top)
        root.addLayout(btnhlay)

        stats = QHBoxLayout()
        self.lbl_clients = QLabel("Aktive Clients: 0")
        self.lbl_requests = QLabel("Requests: 0")
        self.lbl_domains = QLabel("Domains: 0")
        self.lbl_autosave = QLabel("Autosave: -")
        self.lbl_reports = QLabel("Reports: -")
        for w in [self.lbl_clients, self.lbl_requests, self.lbl_domains, self.lbl_autosave, self.lbl_reports]:
            stats.addWidget(w)
            stats.addSpacing(20)
        stats.addStretch(1)
        root.addLayout(stats)

        self.info = QTextEdit()
        self.info.setReadOnly(True)
        root.addWidget(self.info)
        self.setLayout(root)

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.refresh)
        self.timer.start(5000)
        self.refresh()

    def service_name(self):
        return DB.setting("squid_service_name", "Squid")

    def squid_binary(self):
        return DB.setting("squid_binary", "squid")

    def squid_conf_path(self):
        return DB.setting("squid_conf_path", str(DEFAULT_SQUID_CONF))

    def _run_sc(self, action):
        try:
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= startupinfo.STARTF_USESHOWWINDOW
            
                p = subprocess.Popen(
                    ["sc", action, self.service_name()],
                    creationflags  = subprocess.CREATE_NO_WINDOW,
                    startupinfo    = startupinfo,
                    capture_output = True,
                    text           = True,
                    stdout         = subprocess.PIPE,
                    stderr         = subprocess.PIPE
                )
                out, err = p.communicate(timeout=60)
                if p.returncode != 0:
                    raise RuntimeError(f"SC failed ({p.returncode}):\n{err}")
                return p.stdout + "\n" + p.stderr
            else:
                content = "Squid is only available under Microsoft Windows"
                dlg = ErrorMessage(
                    title    = "Laufzeitfehler",
                    message  = content,
                    log_path = LOG,
                    parent   = MAINWIN
                )
                dlg.exec_()
                return False
        except Exception as e:
            return str(e)

    def start_service(self):
        self.message("Dienst", self._run_sc("start"))
        self.refresh()

    def stop_service(self):
        self.message("Dienst", self._run_sc("stop"))
        self.refresh()

    def restart_service(self):
        text = self._run_sc("stop")
        time.sleep(1.5)
        text += "\n" + self._run_sc("start")
        self.message("Dienst", text)
        self.refresh()

    def reload_service(self):
        try:
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= startupinfo.STARTF_USESHOWWINDOW
            
                p = subprocess.Popen(
                    [self.squid_binary(), "-k", "reconfigure", "-f", self.squid_conf_path()],
                    capture_output = True,
                    text           = True,
                    creationflags  = subprocess.CREATE_NO_WINDOW,
                    startupinfo    = startupinfo,
                    stdout         = subprocess.PIPE,
                    stderr         = subprocess.PIPE
                )
                out, err = p.communicate(timeout=60)
                if p.returncode != 0:
                    raise RuntimeError(f"Squid failed ({p.returncode}):\n{err}")
                self.message("Reload", p.stdout + "\n" + p.stderr)
                self.refresh()
                return True
            else:
                content = "Squid is only available under Microsoft Windows"
                dlg = ErrorMessage(
                    title    = "Laufzeitfehler",
                    message  = content,
                    log_path = LOG,
                    parent   = MAINWIN
                )
                dlg.exec_()
                return False
        except Exception as e:
            self.warn("Fehler", str(e))
            return False

    def test_config(self):
        try:
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= startupinfo.STARTF_USESHOWWINDOW
                
                p = subprocess.Popen(
                    [self.squid_binary(), "-k", "parse", "-f", self.squid_conf_path()],
                    capture_output = True,
                    text           = True,
                    creationflags  = subprocess.CREATE_NO_WINDOW,
                    startupinfo    = startupinfo,
                    stdout         = subprocess.PIPE,
                    stderr         = subprocess.PIPE
                )
                out, err = p.communicate(timeout=60)
                if p.returncode != 0:
                    raise RuntimeError(f"Squid failed ({p.returncode}):\n{err}")
                self.message("Konfigurationstest", p.stdout + "\n" + p.stderr)
                return True
            else:
                content = "Squid is only available under Microsoft Windows"
                dlg = ErrorMessage(
                    title    = "Laufzeitfehler",
                    message  = content,
                    log_path = LOG,
                    parent   = MAINWIN
                )
                dlg.exec_()
                return False
        except Exception as e:
            self.warn("Fehler", str(e))
            return False

    def refresh(self):
        try:
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= startupinfo.STARTF_USESHOWWINDOW
                
                p = subprocess.Popen(
                    ["sc", "query", self.service_name()],
                    capture_output = True,
                    text           = True,
                    creationflags  = subprocess.CREATE_NO_WINDOW,
                    startupinfo    = startupinfo,
                    stdout         = subprocess.PIPE,
                    stderr         = subprocess.PIPE
                )
                out, err = p.communicate(timeout=60)
                if p.returncode != 0:
                    raise RuntimeError(f"Squid failed ({p.returncode}):\n{err}")
                
                if "RUNNING" in p.stdout:
                    self.service_led.set_green()
                elif "STOPPED" in p.stdout:
                    self.service_led.set_red()
                else:
                    self.service_led.set_yellow()
            else:
                content = "Squid is only available under Microsoft Windows"
                dlg = ErrorMessage(
                    title    = "Laufzeitfehler",
                    message  = content,
                    log_path = LOG,
                    parent   = MAINWIN
                )
                dlg.exec_()
                return False
        except Exception:
            self.service_led.set_red()
            return False

        access_log_path = Path(DB.setting("access_log_path", str(DEFAULT_ACCESS_LOG)))
        recent = [parse_access_log_line(x) for x in read_tail_lines(access_log_path, 500)]
        recent = [x for x in recent if x]
        cutoff = datetime.now() - timedelta(minutes=60)
        recent = [x for x in recent if x["timestamp"] >= cutoff]

        actors = {}
        domains = set()
        for r in recent:
            key = r["username"] if r["username"] != "-" else r["client_ip"]
            actors[key] = r
            domains.add(r["domain"])

        self.activity_led.set_green() if recent else self.activity_led.set_red()
        self.lbl_clients.setText(f"Aktive Clients: {len(actors)}")
        self.lbl_requests.setText(f"Requests: {len(recent)}")
        self.lbl_domains.setText(f"Domains: {len(domains)}")
        self.lbl_autosave.setText(f"Autosave: {DB.setting('last_autosave', '-') or '-'}")

        report_dir = Path(DB.setting("report_output_dir", str(APP_DIR / "reports")))
        reports = len(list(report_dir.glob("*.html"))) if report_dir.exists() else 0
        self.lbl_reports.setText(f"HTML Reports: {reports}")

        lines = [
            f"Zeit: {now_iso()}",
            f"Access-Log: {access_log_path}",
            f"squid.conf: {self.squid_conf_path()}",
            "",
            "Zuletzt aktive Clients:",
        ]
        for _, r in list(actors.items())[:15]:
            lines.append(f"- {r['timestamp'].strftime('%Y-%m-%d %H:%M:%S')} | {r['username']} | {r['client_ip']} | {r['domain']}")
        self.info.setPlainText("\n".join(lines))


class ReplacementPagesTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()

        self.ed_name     = QLineEdit()
        self.cb_category = QComboBox()
        self.cb_category.addItems([""] + URL_CATEGORIES)
        self.ed_file     = QLineEdit()
        self.ed_comment  = QLineEdit()
        self.ed_comment.setMaximumWidth(600)
        self.chk_enabled = QCheckBox("Aktiv")
        self.chk_enabled.setChecked(True)

        btn_pick = QPushButton("...")
        btn_pick.clicked.connect(self.pick_file)

        form1 = QHBoxLayout()
        for w1 in [QLabel("Name"     ), self.ed_name,
                   QLabel("Kategorie"), self.cb_category,
                   QLabel("Datei"    ), self.ed_file, btn_pick]:
            form1.addWidget(w1)
        
        form2 = QHBoxLayout()
        for w2 in [QLabel("Kommentar"), self.ed_comment, self.chk_enabled]:
            form2.addWidget(w2)

        form3 = QHBoxLayout()
        for text, fn in [("Hinzufügen"   , self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen"      , self.delete_selected),
                         ("Neu laden"    , self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form3.addWidget(b)

        root.addLayout(form)
        root.addLayout(form1)
        root.addLayout(form2)
        root.addLayout(form3)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.load()

    def pick_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Ersatz-Page auswählen", str(APP_DIR), "HTML Dateien (*.html *.htm);;Alle Dateien (*)")
        if path:
            self.ed_file.setText(path)

    def clear_form(self):
        self.ed_name.clear()
        self.ed_file.clear()
        self.ed_comment.clear()
        self.chk_enabled.setChecked(True)
        self.cb_category.setCurrentIndex(0)

    def add_row(self):
        DB.execute("INSERT INTO replacement_pages(name, category, file_path, is_enabled, comment) VALUES(?,?,?,?,?)",
                   (self.ed_name.text().strip(), self.cb_category.currentText(), self.ed_file.text().strip(),
                    1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("UPDATE replacement_pages SET name=?, category=?, file_path=?, is_enabled=?, comment=? WHERE id=?",
                   (self.ed_name.text().strip(), self.cb_category.currentText(), self.ed_file.text().strip(),
                    1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM replacement_pages WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_name.setText(item_text(self.table, row, 1))
        idx = self.cb_category.findText(item_text(self.table, row, 2))
        self.cb_category.setCurrentIndex(idx if idx >= 0 else 0)
        self.ed_file.setText(item_text(self.table, row, 3))
        self.chk_enabled.setChecked(item_text(self.table, row, 4) == "1")
        self.ed_comment.setText(item_text(self.table, row, 5))

    def load(self):
        rows = DB.fetchall("SELECT id, name, category, file_path, is_enabled, comment FROM replacement_pages ORDER BY name")
        self.fill_table(self.table, ["ID", "Name", "Kategorie", "Datei", "Aktiv", "Kommentar"], rows)

class UrlFilterTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()

        self.ed_pattern     = QLineEdit()
        self.cb_category    = QComboBox()
        self.cb_category.addItems(URL_CATEGORIES)
        self.cb_replacement = QComboBox()
        self.ed_comment     = QLineEdit()
        self.ed_comment.setMaximumWidth(800)
        self.chk_regex      = QCheckBox("Regex")
        self.chk_enabled    = QCheckBox("Aktiv")
        self.chk_enabled.setChecked(True)
        
        form2 = QHBoxLayout()
        for w2 in [QLabel("Muster"     ), self.ed_pattern,
                  QLabel("Kategorie"  ), self.cb_category,
                  QLabel("Ersatz-Page"), self.cb_replacement]:
            form2.addWidget(w2)
        
        form3 = QHBoxLayout()
        for w3 in [QLabel("Kommentar"  ), self.ed_comment,
                   self.chk_regex, self.chk_enabled]:
            form3.addWidget(w3)
            
        form4 = QHBoxLayout()
        for text, fn in [("Hinzufügen"   , self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen"      , self.delete_selected),
                         ("Export"       , self.export_file),
                         ("Neu laden"    , self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form4.addWidget(b)

        root.addLayout(form)
        root.addLayout(form2)
        root.addLayout(form3)
        root.addLayout(form4)

        nav = QHBoxLayout()
        self.limit_combo = QComboBox()
        self.limit_combo.addItems(["20", "50", "100", "200"])
        self.offset_spin = QSpinBox()
        self.offset_spin.setRange(0, 1000000)
        btn_show = QPushButton("Anzeigen")
        btn_show.clicked.connect(self.load)
        nav.addWidget(QLabel("Limit"))
        nav.addWidget(self.limit_combo)
        nav.addWidget(QLabel("Offset"))
        nav.addWidget(self.offset_spin)
        nav.addWidget(btn_show)
        nav.addStretch(1)
        root.addLayout(nav)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.refresh_replacements()
        self.load()

    def refresh_replacements(self):
        current = self.cb_replacement.currentText()
        self.cb_replacement.clear()
        self.cb_replacement.addItem("-", None)
        for rid, name in DB.fetchall("SELECT id, name FROM replacement_pages WHERE is_enabled=1 ORDER BY name"):
            self.cb_replacement.addItem(name, rid)
        idx = self.cb_replacement.findText(current)
        self.cb_replacement.setCurrentIndex(idx if idx >= 0 else 0)

    def clear_form(self):
        self.ed_pattern.clear()
        self.ed_comment.clear()
        self.chk_regex.setChecked(False)
        self.chk_enabled.setChecked(True)
        self.cb_category.setCurrentIndex(0)
        self.cb_replacement.setCurrentIndex(0)

    def add_row(self):
        DB.execute("""
            INSERT INTO blocked_urls(pattern, category, is_regex, is_enabled, replacement_page_id, comment)
            VALUES(?,?,?,?,?,?)
        """, (self.ed_pattern.text().strip(), self.cb_category.currentText(),
              1 if self.chk_regex.isChecked() else 0, 1 if self.chk_enabled.isChecked() else 0,
              self.cb_replacement.currentData(), self.ed_comment.text().strip()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("""
            UPDATE blocked_urls
               SET pattern=?, category=?, is_regex=?, is_enabled=?, replacement_page_id=?, comment=?
             WHERE id=?
        """, (self.ed_pattern.text().strip(), self.cb_category.currentText(),
              1 if self.chk_regex.isChecked() else 0, 1 if self.chk_enabled.isChecked() else 0,
              self.cb_replacement.currentData(), self.ed_comment.text().strip(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM blocked_urls WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_pattern.setText(item_text(self.table, row, 1))
        idx = self.cb_category.findText(item_text(self.table, row, 2))
        self.cb_category.setCurrentIndex(idx if idx >= 0 else 0)
        self.chk_regex.setChecked(item_text(self.table, row, 3) == "1")
        self.chk_enabled.setChecked(item_text(self.table, row, 4) == "1")
        idx = self.cb_replacement.findText(item_text(self.table, row, 5))
        self.cb_replacement.setCurrentIndex(idx if idx >= 0 else 0)
        self.ed_comment.setText(item_text(self.table, row, 6))

    def export_file(self):
        rows = DB.fetchall("SELECT pattern FROM blocked_urls WHERE is_enabled=1 ORDER BY id")
        with open(BLOCKED_FILE, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(r[0] + "\n")
        self.message("Export", f"{BLOCKED_FILE.name} wurde geschrieben.")

    def load(self):
        self.refresh_replacements()
        limit = int(self.limit_combo.currentText())
        offset = int(self.offset_spin.value())
        rows = DB.fetchall("""
            SELECT b.id, b.pattern, b.category, b.is_regex, b.is_enabled, COALESCE(r.name,''), b.comment, b.created_at
              FROM blocked_urls b
         LEFT JOIN replacement_pages r ON r.id=b.replacement_page_id
          ORDER BY b.id
             LIMIT ? OFFSET ?
        """, (limit, offset))
        self.fill_table(self.table, ["ID", "Pattern", "Kategorie", "Regex", "Aktiv", "Ersatz-Page", "Kommentar", "Erstellt"], rows)

class GroupsTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()
        self.ed_name     = QLineEdit()
        self.ed_comment  = QLineEdit()
        self.chk_enabled = QCheckBox("Aktiv")
        self.chk_enabled.setChecked(True)
        for w in [QLabel("Gruppe"   ), self.ed_name,
                  QLabel("Kommentar"), self.ed_comment, self.chk_enabled]:
            form.addWidget(w)
        
        form2 = QHBoxLayout()
        for text, fn in [("Hinzufügen", self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen", self.delete_selected),
                         ("Neu laden", self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form2.addWidget(b)
            
        root.addLayout(form)
        root.addLayout(form2)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.load()

    def clear_form(self):
        self.ed_name.clear()
        self.ed_comment.clear()
        self.chk_enabled.setChecked(True)

    def add_row(self):
        DB.execute("INSERT INTO groups(name, is_enabled, comment) VALUES(?,?,?)",
                   (self.ed_name.text().strip(), 1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("UPDATE groups SET name=?, is_enabled=?, comment=? WHERE id=?",
                   (self.ed_name.text().strip(), 1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM groups WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_name.setText(item_text(self.table, row, 1))
        self.chk_enabled.setChecked(item_text(self.table, row, 2) == "1")
        self.ed_comment.setText(item_text(self.table, row, 3))

    def load(self):
        rows = DB.fetchall("SELECT id, name, is_enabled, comment FROM groups ORDER BY name")
        self.fill_table(self.table, ["ID", "Name", "Aktiv", "Kommentar"], rows)

class NetworksTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()
        self.ed_name = QLineEdit()
        self.ed_cidr = QLineEdit()
        self.ed_comment = QLineEdit()
        self.chk_enabled = QCheckBox("Aktiv")
        self.chk_enabled.setChecked(True)
        for w in [QLabel("Name"), self.ed_name, QLabel("CIDR"), self.ed_cidr, QLabel("Kommentar"), self.ed_comment, self.chk_enabled]:
            form.addWidget(w)
        
        form2 = QHBoxLayout()
        for text, fn in [("Hinzufügen"   , self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen"      , self.delete_selected),
                         ("Neu laden"    , self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form2.addWidget(b)
            
        root.addLayout(form)
        root.addLayout(form2)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.load()

    def clear_form(self):
        self.ed_name.clear()
        self.ed_cidr.clear()
        self.ed_comment.clear()
        self.chk_enabled.setChecked(True)

    def add_row(self):
        try:
            ipaddress.ip_network(self.ed_cidr.text().strip(), strict=False)
        except Exception:
            self.warn("Fehler", "Ungültiges CIDR-Format.")
            return
        DB.execute("INSERT INTO networks(name, cidr, is_enabled, comment) VALUES(?,?,?,?)",
                   (self.ed_name.text().strip(), self.ed_cidr.text().strip(),
                    1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        try:
            ipaddress.ip_network(self.ed_cidr.text().strip(), strict=False)
        except Exception:
            self.warn("Fehler", "Ungültiges CIDR-Format.")
            return
        DB.execute("UPDATE networks SET name=?, cidr=?, is_enabled=?, comment=? WHERE id=?",
                   (self.ed_name.text().strip(), self.ed_cidr.text().strip(),
                    1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM networks WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_name.setText(item_text(self.table, row, 1))
        self.ed_cidr.setText(item_text(self.table, row, 2))
        self.chk_enabled.setChecked(item_text(self.table, row, 3) == "1")
        self.ed_comment.setText(item_text(self.table, row, 4))

    def load(self):
        rows = DB.fetchall("SELECT id, name, cidr, is_enabled, comment FROM networks ORDER BY name")
        self.fill_table(self.table, ["ID", "Name", "CIDR", "Aktiv", "Kommentar"], rows)

class CheckableComboBox(QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setModel(QStandardItemModel(self))
        self.view().pressed.connect(self.handle_item_pressed)
        self.setEditable(True)
        self.lineEdit().setReadOnly(True)
        self.lineEdit().setPlaceholderText("Bitte auswählen ...")
        self.update_text()

    def add_check_item(self, text, checked=False):
        item = QStandardItem(text)
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
        item.setData(Qt.Checked if checked else Qt.Unchecked, Qt.CheckStateRole)
        self.model().appendRow(item)
        self.update_text()

    def handle_item_pressed(self, index):
        item = self.model().itemFromIndex(index)
        if item.checkState() == Qt.Checked:
            item.setCheckState(Qt.Unchecked)
        else:
            item.setCheckState(Qt.Checked)
        self.update_text()

    def checked_items(self):
        result = []
        for row in range(self.model().rowCount()):
            item = self.model().item(row)
            if item.checkState() == Qt.Checked:
                result.append(item.text())
        return result

    def update_text(self):
        items = self.checked_items()
        self.lineEdit().setText(", ".join(items))

class TimeWindowsTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()
        
        self.ed_name     = QLineEdit()
        self.cb_weekdays = CheckableComboBox()
        self.cb_weekdays.add_check_item("Montag")
        self.cb_weekdays.add_check_item("Dienstag")
        self.cb_weekdays.add_check_item("Mittwoch")
        self.cb_weekdays.add_check_item("Donnerstag")
        self.cb_weekdays.add_check_item("Freitag")
        self.cb_weekdays.add_check_item("Samstag")
        self.cb_weekdays.add_check_item("Sonntag")

        self.cb_start    = TimeComboBox()
        self.cb_end      = TimeComboBox()
        self.ed_comment  = QLineEdit()
        
        self.chk_enabled = QCheckBox("Aktiv")
        self.chk_enabled.setChecked(True)
        
        for w in [QLabel("Name"      ), self.ed_name,
                  QLabel("Wochentage"), self.cb_weekdays,
                  QLabel("Von"       ), self.cb_start,
                  QLabel("Bis"       ), self.cb_end,
                  QLabel("Kommentar" ), self.ed_comment, self.chk_enabled]:
            form.addWidget(w)
            
        form2 = QHBoxLayout()
        for text, fn in [("Hinzufügen"   , self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen"      , self.delete_selected),
                         ("Neu laden"    , self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form2.addWidget(b)
            
        root.addLayout(form)
        root.addLayout(form2)
        
        root.addWidget(QLabel("Beispiel: mon,tue,wed,thu,fri | 08:00 | 17:30"))
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.load()

    def clear_form(self):
        self.ed_name.clear()
        self.ed_weekdays.clear()
        self.ed_start.clear()
        self.ed_end.clear()
        self.ed_comment.clear()
        self.chk_enabled.setChecked(True)

    def add_row(self):
        DB.execute("INSERT INTO time_windows(name, weekdays, start_time, end_time, is_enabled, comment) VALUES(?,?,?,?,?,?)",
                   (self.ed_name.text().strip(), self.ed_weekdays.text().strip(), self.ed_start.text().strip(),
                    self.ed_end.text().strip(), 1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("UPDATE time_windows SET name=?, weekdays=?, start_time=?, end_time=?, is_enabled=?, comment=? WHERE id=?",
                   (self.ed_name.text().strip(), self.ed_weekdays.text().strip(), self.ed_start.text().strip(),
                    self.ed_end.text().strip(), 1 if self.chk_enabled.isChecked() else 0, self.ed_comment.text().strip(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM time_windows WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_name.setText(item_text(self.table, row, 1))
        self.ed_weekdays.setText(item_text(self.table, row, 2))
        self.ed_start.setText(item_text(self.table, row, 3))
        self.ed_end.setText(item_text(self.table, row, 4))
        self.chk_enabled.setChecked(item_text(self.table, row, 5) == "1")
        self.ed_comment.setText(item_text(self.table, row, 6))

    def load(self):
        rows = DB.fetchall("SELECT id, name, weekdays, start_time, end_time, is_enabled, comment FROM time_windows ORDER BY name")
        self.fill_table(self.table, ["ID", "Name", "Wochentage", "Von", "Bis", "Aktiv", "Kommentar"], rows)

class UsersTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()
        
        self.ed_user     = QLineEdit()
        self.ed_password = QLineEdit()
        self.ed_password .setEchoMode(QLineEdit.Password)
        self.ed_cert     = QLineEdit()
        self.cb_group    = QComboBox()
        self.cb_group    .setMaximumWidth(100)
        self.cb_network  = QComboBox()
        self.cb_network  .setMaximumWidth(100)
        self.cb_time     = QComboBox()
        self.cb_time     .setMaximumWidth(100)
        self.chk_enabled = QCheckBox("Aktiv")
        self.chk_enabled .setMaximumWidth(64)
        self.chk_enabled.setChecked(True)
        self.chk_blocked = QCheckBox("Blockiert")
        
        form1 = QHBoxLayout()
        for w1 in [QLabel("Benutzer" ), self.ed_user,
                   QLabel("Passwort" ), self.ed_password]:
            form1.addWidget(w1)
        
        form2 = QHBoxLayout()
        for w2 in [QLabel("Zertifikat"), self.ed_cert]:
            form2.addWidget(w2)
            
        form3 = QHBoxLayout()
        for w3 in [QLabel("Gruppe"    ), self.cb_group,
                   QLabel("Netz"      ), self.cb_network,
                   QLabel("Zeit"      ), self.cb_time,
                   self.chk_enabled, self.chk_blocked]:
            form3.addWidget(w3)

        form4 = QHBoxLayout()
        for text, fn in [("Hinzufügen"   , self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen"      , self.delete_selected),
                         ("Block switch" , self.toggle_block),
                         ("Neu laden"    , self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form4.addWidget(b)

        root.addLayout(form1)
        root.addLayout(form2)
        root.addLayout(form3)
        root.addLayout(form4)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.refresh_refs()
        self.load()

    def refresh_refs(self):
        current_group = self.cb_group.currentText()
        current_network = self.cb_network.currentText()
        current_time = self.cb_time.currentText()
        self.cb_group.clear()
        self.cb_network.clear()
        self.cb_time.clear()
        self.cb_group.addItem("-", None)
        self.cb_network.addItem("-", None)
        self.cb_time.addItem("-", None)
        for gid, name in DB.fetchall("SELECT id, name FROM groups ORDER BY name"):
            self.cb_group.addItem(name, gid)
        for nid, name in DB.fetchall("SELECT id, name FROM networks ORDER BY name"):
            self.cb_network.addItem(name, nid)
        for tid, name in DB.fetchall("SELECT id, name FROM time_windows ORDER BY name"):
            self.cb_time.addItem(name, tid)
        for combo, text in [(self.cb_group, current_group), (self.cb_network, current_network), (self.cb_time, current_time)]:
            idx = combo.findText(text)
            combo.setCurrentIndex(idx if idx >= 0 else 0)

    def clear_form(self):
        self.ed_user.clear()
        self.ed_password.clear()
        self.ed_cert.clear()
        self.chk_enabled.setChecked(True)
        self.chk_blocked.setChecked(False)
        self.cb_group.setCurrentIndex(0)
        self.cb_network.setCurrentIndex(0)
        self.cb_time.setCurrentIndex(0)

    def add_row(self):
        DB.execute("""
            INSERT INTO users(username, password_hash, cert_fingerprint, is_enabled, is_blocked, group_id, network_id, time_window_id)
            VALUES(?,?,?,?,?,?,?,?)
        """, (self.ed_user.text().strip(),
              hash_password(self.ed_password.text().strip()) if self.ed_password.text().strip() else "",
              self.ed_cert.text().strip(), 1 if self.chk_enabled.isChecked() else 0,
              1 if self.chk_blocked.isChecked() else 0, self.cb_group.currentData(),
              self.cb_network.currentData(), self.cb_time.currentData()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        password_hash = item_text(self.table, row, 8)
        if self.ed_password.text().strip():
            password_hash = hash_password(self.ed_password.text().strip())
        DB.execute("""
            UPDATE users SET username=?, password_hash=?, cert_fingerprint=?, is_enabled=?, is_blocked=?, group_id=?, network_id=?, time_window_id=? WHERE id=?
        """, (self.ed_user.text().strip(), password_hash, self.ed_cert.text().strip(),
              1 if self.chk_enabled.isChecked() else 0, 1 if self.chk_blocked.isChecked() else 0,
              self.cb_group.currentData(), self.cb_network.currentData(), self.cb_time.currentData(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM users WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def toggle_block(self):
        row = self.table.currentRow()
        if row < 0:
            return
        new_val = 0 if item_text(self.table, row, 5) == "1" else 1
        DB.execute("UPDATE users SET is_blocked=? WHERE id=?", (new_val, item_text(self.table, row, 0)))
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_user.setText(item_text(self.table, row, 1))
        self.ed_password.clear()
        self.ed_cert.setText(item_text(self.table, row, 9))
        self.chk_blocked.setChecked(item_text(self.table, row, 5) == "1")
        self.chk_enabled.setChecked(item_text(self.table, row, 6) == "1")
        for combo, text in [(self.cb_group, item_text(self.table, row, 2)), (self.cb_network, item_text(self.table, row, 3)), (self.cb_time, item_text(self.table, row, 4))]:
            idx = combo.findText(text)
            combo.setCurrentIndex(idx if idx >= 0 else 0)

    def load(self):
        self.refresh_refs()
        rows = DB.fetchall("""
            SELECT u.id, u.username, COALESCE(g.name,''), COALESCE(n.name,''), COALESCE(t.name,''),
                   u.is_blocked, u.is_enabled, u.created_at, u.password_hash, u.cert_fingerprint
              FROM users u
         LEFT JOIN groups g ON g.id=u.group_id
         LEFT JOIN networks n ON n.id=u.network_id
         LEFT JOIN time_windows t ON t.id=u.time_window_id
          ORDER BY u.username
        """)
        self.fill_table(self.table, ["ID", "Username", "Gruppe", "Netz", "Zeitfenster", "Blockiert", "Aktiv", "Erstellt", "Hash", "Zertifikat"], rows)

class BehaviorRulesTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QHBoxLayout()

        self.ed_name        = QLineEdit()
        self.ed_pattern     = QLineEdit()
        self.cb_category    = QComboBox()
        self.cb_category.addItems([""] + URL_CATEGORIES)
        self.chk_regex      = QCheckBox("Regex")
        self.cb_scope_type  = QComboBox()
        self.cb_scope_type.addItems(["Alle", "Benutzer", "Gruppe"])
        self.cb_scope_value = QComboBox()
        self.cb_scope_value.setMinimumWidth(100)
        self.spin_window    = QSpinBox()
        self.spin_window.setRange(1, 10080)
        self.spin_window.setValue(60)
        self.spin_threshold = QSpinBox()
        self.spin_threshold.setRange(1, 100000)
        self.spin_threshold.setValue(10)
        self.chk_enabled    = QCheckBox("Aktiv")
        self.chk_enabled.setChecked(True)
        self.ed_comment     = QLineEdit()

        self.cb_scope_type.currentTextChanged.connect(self.refresh_scope_values)
        self.refresh_scope_values()

        form2 = QHBoxLayout()
        for w2 in [QLabel("Name"      ), self.ed_name,
                   QLabel("URL Muster"), self.ed_pattern,
                   QLabel("Kategorie" ), self.cb_category, self.chk_regex ]:
            form2.addWidget(w2)
        
        form3 = QHBoxLayout()        
        for w3 in [QLabel("Scope"      ), self.cb_scope_type , self.cb_scope_value,
                   QLabel("Fenster Min"), self.spin_window   ,
                   QLabel("Threshold"  ), self.spin_threshold, self.chk_enabled]:
            form3.addWidget(w3)
        
        form4 = QHBoxLayout()
        for w4 in [QLabel("Kommentar"  ), self.ed_comment]:
            form4.addWidget(w4)
            
        form5 = QHBoxLayout()
        for text, fn in [("Hinzufügen"   , self.add_row),
                         ("Aktualisieren", self.update_selected),
                         ("Löschen"      , self.delete_selected),
                         ("Neu laden"    , self.load)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            form5.addWidget(b)

        root.addLayout(form)
        root.addLayout(form2)
        root.addLayout(form3)
        root.addLayout(form4)
        root.addLayout(form5)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.table.itemSelectionChanged.connect(self.load_form)
        root.addWidget(self.table)
        self.setLayout(root)
        self.load()

    def refresh_scope_values(self):
        current = self.cb_scope_value.currentText()
        self.cb_scope_value.clear()
        scope = self.cb_scope_type.currentText()
        if scope == "Alle":
            self.cb_scope_value.addItem("", "")
        elif scope == "Benutzer":
            for (u,) in DB.fetchall("SELECT username FROM users ORDER BY username"):
                self.cb_scope_value.addItem(u, u)
        elif scope == "Gruppe":
            for (_, name) in DB.fetchall("SELECT id, name FROM groups ORDER BY name"):
                self.cb_scope_value.addItem(name, name)
        idx = self.cb_scope_value.findText(current)
        self.cb_scope_value.setCurrentIndex(idx if idx >= 0 else 0)

    def clear_form(self):
        self.ed_name.clear()
        self.ed_pattern.clear()
        self.cb_category.setCurrentIndex(0)
        self.chk_regex.setChecked(False)
        self.cb_scope_type.setCurrentIndex(0)
        self.refresh_scope_values()
        self.spin_window.setValue(60)
        self.spin_threshold.setValue(10)
        self.chk_enabled.setChecked(True)
        self.ed_comment.clear()

    def add_row(self):
        DB.execute("""
            INSERT INTO behavior_rules(name, url_pattern, category, is_regex, scope_type, scope_value, window_minutes, threshold_count, is_enabled, comment)
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (self.ed_name.text().strip(), self.ed_pattern.text().strip(), self.cb_category.currentText(),
              1 if self.chk_regex.isChecked() else 0, self.cb_scope_type.currentText(), self.cb_scope_value.currentText(),
              self.spin_window.value(), self.spin_threshold.value(), 1 if self.chk_enabled.isChecked() else 0,
              self.ed_comment.text().strip()))
        self.clear_form()
        self.load()

    def update_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("""
            UPDATE behavior_rules
               SET name=?, url_pattern=?, category=?, is_regex=?, scope_type=?, scope_value=?, window_minutes=?, threshold_count=?, is_enabled=?, comment=?
             WHERE id=?
        """, (self.ed_name.text().strip(), self.ed_pattern.text().strip(), self.cb_category.currentText(),
              1 if self.chk_regex.isChecked() else 0, self.cb_scope_type.currentText(), self.cb_scope_value.currentText(),
              self.spin_window.value(), self.spin_threshold.value(), 1 if self.chk_enabled.isChecked() else 0,
              self.ed_comment.text().strip(), item_text(self.table, row, 0)))
        self.load()

    def delete_selected(self):
        row = self.table.currentRow()
        if row < 0:
            return
        DB.execute("DELETE FROM behavior_rules WHERE id=?", (item_text(self.table, row, 0),))
        self.clear_form()
        self.load()

    def load_form(self):
        row = self.table.currentRow()
        if row < 0:
            return
        self.ed_name.setText(item_text(self.table, row, 1))
        self.ed_pattern.setText(item_text(self.table, row, 2))
        idx = self.cb_category.findText(item_text(self.table, row, 3))
        self.cb_category.setCurrentIndex(idx if idx >= 0 else 0)
        self.chk_regex.setChecked(item_text(self.table, row, 4) == "1")
        idx = self.cb_scope_type.findText(item_text(self.table, row, 5))
        self.cb_scope_type.setCurrentIndex(idx if idx >= 0 else 0)
        self.refresh_scope_values()
        idx = self.cb_scope_value.findText(item_text(self.table, row, 6))
        self.cb_scope_value.setCurrentIndex(idx if idx >= 0 else 0)
        self.spin_window.setValue(int(item_text(self.table, row, 7) or "60"))
        self.spin_threshold.setValue(int(item_text(self.table, row, 8) or "10"))
        self.chk_enabled.setChecked(item_text(self.table, row, 9) == "1")
        self.ed_comment.setText(item_text(self.table, row, 10))

    def load(self):
        self.refresh_scope_values()
        rows = DB.fetchall("""
            SELECT id, name, url_pattern, category, is_regex, scope_type, scope_value, window_minutes, threshold_count, is_enabled, comment
              FROM behavior_rules
          ORDER BY name
        """)
        self.fill_table(self.table, ["ID", "Name", "URL Muster", "Kategorie", "Regex", "Scope", "Wert", "Fenster", "Threshold", "Aktiv", "Kommentar"], rows)

class LiveClientsTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        top = QHBoxLayout()
        self.spin_minutes = QSpinBox()
        self.spin_minutes.setRange(1, 1440)
        self.spin_minutes.setValue(30)
        btn = QPushButton("Aktualisieren")
        btn.clicked.connect(self.load)
        top.addWidget(QLabel("Zeitraum (Minuten)"))
        top.addWidget(self.spin_minutes)
        top.addWidget(btn)
        top.addStretch(1)
        root.addLayout(top)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        root.addWidget(self.table)
        self.setLayout(root)

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.load)
        self.timer.start(7000)
        self.load()

    def load(self):
        access_log_path = Path(DB.setting("access_log_path", str(DEFAULT_ACCESS_LOG)))
        rows = [parse_access_log_line(x) for x in read_tail_lines(access_log_path, 2000)]
        rows = [x for x in rows if x]
        cutoff = datetime.now() - timedelta(minutes=self.spin_minutes.value())
        rows = [x for x in rows if x["timestamp"] >= cutoff]

        grouped = {}
        for row in rows:
            key = (row["username"], row["client_ip"])
            info = grouped.setdefault(key, {"requests": 0, "last_seen": row["timestamp"], "domain": row["domain"]})
            info["requests"] += 1
            if row["timestamp"] > info["last_seen"]:
                info["last_seen"] = row["timestamp"]
                info["domain"] = row["domain"]

        out = []
        for (username, ip), info in sorted(grouped.items(), key=lambda kv: kv[1]["last_seen"], reverse=True):
            out.append((username, ip, info["requests"], info["last_seen"].strftime("%Y-%m-%d %H:%M:%S"), info["domain"]))
        self.fill_table(self.table, ["Benutzer", "IP", "Requests", "Letzter Zugriff", "Letzte Domain"], out)

class LogsTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        top = QHBoxLayout()
        self.ed_filter = QLineEdit()
        self.spin_lines = QSpinBox()
        self.spin_lines.setRange(50, 10000)
        self.spin_lines.setValue(300)
        btn_access = QPushButton("Access Log")
        btn_cache = QPushButton("Cache Log")
        btn_access.clicked.connect(lambda: self.load("access"))
        btn_cache.clicked.connect(lambda: self.load("cache"))
        top.addWidget(QLabel("Filter"))
        top.addWidget(self.ed_filter)
        top.addWidget(QLabel("Zeilen"))
        top.addWidget(self.spin_lines)
        top.addWidget(btn_access)
        top.addWidget(btn_cache)
        root.addLayout(top)
        self.text = QTextEdit()
        self.text.setReadOnly(True)
        root.addWidget(self.text)
        self.setLayout(root)
        self.mode = "access"
        self.load("access")

    def load(self, mode=None):
        if mode:
            self.mode = mode
        path = Path(DB.setting("access_log_path", str(DEFAULT_ACCESS_LOG))) if self.mode == "access" else Path(DB.setting("cache_log_path", str(DEFAULT_CACHE_LOG)))
        lines = read_tail_lines(path, self.spin_lines.value())
        flt = self.ed_filter.text().strip().lower()
        if flt:
            lines = [x for x in lines if flt in x.lower()]
        self.text.setPlainText("".join(lines))


class StatisticsTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        self.last_report_data = None

        root = QVBoxLayout()
        controls = QHBoxLayout()
        self.spin_minutes = QSpinBox()
        self.spin_minutes.setRange(5, 43200)
        self.spin_minutes.setValue(60)

        self.cb_bucket = QComboBox()
        self.cb_bucket.addItems(["5", "15", "60", "240", "1440"])

        self.cb_chart = QComboBox()
        self.cb_chart.addItems(["Top URLs", "Top Benutzer", "Top Domains"])

        form2 = QHBoxLayout()
        btn_refresh = QPushButton("Berechnen")
        btn_import  = QPushButton("Access-Log importieren")
        btn_csv     = QPushButton("CSV Export")
        btn_html    = QPushButton("HTML Report")
        btn_bar     = QPushButton("Balken-Diagramm")
        btn_pie     = QPushButton("Kreis-Diagramm")
        
        btn_refresh.clicked.connect(self.load)
        btn_import .clicked.connect(self.import_events)
        btn_csv    .clicked.connect(self.export_csv_bundle)
        btn_html   .clicked.connect(self.export_html_report)
        btn_bar    .clicked.connect(self.draw_bar_chart)
        btn_pie    .clicked.connect(self.draw_pie_chart)

        form2.addWidget(btn_refresh)
        form2.addWidget(btn_import)
        form2.addWidget(btn_csv)
        form2.addWidget(btn_html)
        form2.addWidget(btn_bar)
        form2.addWidget(btn_pie)
        
        controls.addWidget(QLabel("Zeitraum (Minuten)"))
        controls.addWidget(self.spin_minutes)
        controls.addWidget(QLabel("Trend-Bucket (Minuten)"))
        controls.addWidget(self.cb_bucket)
        controls.addWidget(QLabel("Diagramm"))
        controls.addWidget(self.cb_chart)
        controls.addWidget(btn_refresh)
        controls.addWidget(btn_import)
        controls.addWidget(btn_csv)
        controls.addWidget(btn_html)
        controls.addWidget(btn_bar)
        controls.addWidget(btn_pie)
        controls.addStretch(1)
        
        root.addLayout(controls)
        root.addLayout(form2)

        split = QSplitter()
        left = QWidget()
        left_l = QVBoxLayout()
        
        self.tbl_top_urls    = QTableWidget()
        self.tbl_top_urls.setAlternatingRowColors(True)
        self.tbl_top_urls.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.tbl_top_users   = QTableWidget()
        self.tbl_top_users.setAlternatingRowColors(True)
        self.tbl_top_users.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.tbl_top_domains = QTableWidget()
        self.tbl_top_domains.setAlternatingRowColors(True)
        self.tbl_top_domains.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        left_l.addWidget(QLabel("Top URLs"))
        left_l.addWidget(self.tbl_top_urls)
        left_l.addWidget(QLabel("Top Benutzer"))
        left_l.addWidget(self.tbl_top_users)
        left_l.addWidget(QLabel("Top Domains"))
        left_l.addWidget(self.tbl_top_domains)
        
        left.setLayout(left_l)

        right = QWidget()
        right_l = QVBoxLayout()
        
        self.tbl_activity = QTableWidget()
        self.tbl_activity.setAlternatingRowColors(True)
        self.tbl_activity.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.tbl_behavior = QTableWidget()
        self.tbl_behavior.setAlternatingRowColors(True)
        self.tbl_behavior.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.tbl_trend    = QTableWidget()
        self.tbl_trend.setAlternatingRowColors(True)
        self.tbl_trend.setStyleSheet(f"""
        QHeaderView::section {{ color: {AppMode.TableWidgetHeaderColor};}}
        QTableWidget {{
        background-color: {AppMode.TableWidget_BackgroundColor};
        alternate-background-color: {AppMode.TableWidget_AlternateBackgroundColor};
        color: {AppMode.TableWidgetColor};}}""")
        
        self.chart  = MplCanvas()
        self.report = QTextEdit()
        self.report.setReadOnly(True)
        
        right_l.addWidget(QLabel("Letzte Aktivitäten"))
        right_l.addWidget(self.tbl_activity)
        right_l.addWidget(QLabel("Verhaltensmuster Treffer"))
        right_l.addWidget(self.tbl_behavior)
        right_l.addWidget(QLabel("Trend"))
        right_l.addWidget(self.tbl_trend)
        right_l.addWidget(QLabel("Diagramm"))
        right_l.addWidget(self.chart)
        right_l.addWidget(QLabel("Kurzreport"))
        right_l.addWidget(self.report)
        
        right.setLayout(right_l)

        split.addWidget(left)
        split.addWidget(right)
        root.addWidget(split)
        self.setLayout(root)
        self.load()
        self.draw_bar_chart()

    def source_rows(self):
        minutes = self.spin_minutes.value()
        cutoff = datetime.now() - timedelta(minutes=minutes)
        if minutes > 720:
            rows = DB.fetchall("""
                SELECT ts, username, client_ip, method, url, domain, result_code, bytes
                  FROM access_events
                 WHERE ts >= ?
              ORDER BY ts DESC
                 LIMIT 50000
            """, (cutoff.strftime("%Y-%m-%d %H:%M:%S"),))
            parsed = []
            for ts, username, client_ip, method, url, domain, result_code, byte_count in rows:
                try:
                    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    dt = datetime.now()
                parsed.append({
                    "timestamp": dt,
                    "username": username,
                    "client_ip": client_ip,
                    "method": method,
                    "url": url,
                    "domain": domain,
                    "result_code": result_code,
                    "bytes": byte_count or 0,
                })
            return parsed

        access_log_path = Path(DB.setting("access_log_path", str(DEFAULT_ACCESS_LOG)))
        rows = [parse_access_log_line(x) for x in read_tail_lines(access_log_path, 10000)]
        rows = [x for x in rows if x]
        return [x for x in rows if x["timestamp"] >= cutoff]

    def import_events(self):
        access_log_path = Path(DB.setting("access_log_path", str(DEFAULT_ACCESS_LOG)))
        rows = [parse_access_log_line(x) for x in read_tail_lines(access_log_path, 15000)]
        rows = [x for x in rows if x]
        conn = DB.conn()
        cur = conn.cursor()
        inserted = 0
        for row in rows:
            ts = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            exists = cur.execute("SELECT 1 FROM access_events WHERE ts=? AND username=? AND client_ip=? AND url=?",
                                 (ts, row["username"], row["client_ip"], row["url"])).fetchone()
            if exists:
                continue
            cur.execute("""
                INSERT INTO access_events(ts, username, client_ip, method, url, domain, result_code, bytes)
                VALUES(?,?,?,?,?,?,?,?)
            """, (ts, row["username"], row["client_ip"], row["method"], row["url"], row["domain"], row["result_code"], row["bytes"]))
            inserted += 1
        conn.commit()
        conn.close()
        self.message("Import", f"{inserted} Ereignisse importiert.")
        self.load()

    def behavior_hits(self, rows):
        group_map = {u: g for u, g in DB.fetchall("""
            SELECT u.username, COALESCE(g.name, '')
              FROM users u LEFT JOIN groups g ON g.id=u.group_id
        """)}
        out = []
        rules = DB.fetchall("""
            SELECT name, url_pattern, category, is_regex, scope_type, scope_value, window_minutes, threshold_count
              FROM behavior_rules
             WHERE is_enabled=1
          ORDER BY name
        """)
        now = datetime.now()
        for name, pattern, category, is_regex, scope_type, scope_value, window_minutes, threshold_count in rules:
            cutoff = now - timedelta(minutes=int(window_minutes))
            filtered = [r for r in rows if r["timestamp"] >= cutoff]
            match_count = 0
            actors = {}
            for r in filtered:
                try:
                    url_ok = re.search(pattern, r["url"], re.IGNORECASE) is not None if is_regex else pattern.lower() in r["url"].lower()
                except Exception:
                    url_ok = False
                if not url_ok:
                    continue
                actor = r["username"] if r["username"] != "-" else r["client_ip"]
                group = group_map.get(r["username"], "")
                if scope_type == "Benutzer" and scope_value and actor != scope_value:
                    continue
                if scope_type == "Gruppe" and scope_value and group != scope_value:
                    continue
                match_count += 1
                actors[actor] = actors.get(actor, 0) + 1
            if match_count >= int(threshold_count):
                actors_text = ", ".join([f"{k}:{v}" for k, v in sorted(actors.items(), key=lambda x: x[1], reverse=True)[:8]])
                out.append((name, category, scope_type, scope_value, window_minutes, threshold_count, match_count, actors_text))
        return out

    def trend_rows(self, rows):
        bucket_minutes = int(self.cb_bucket.currentText())
        buckets = {}
        for r in rows:
            dt = r["timestamp"]
            minute = (dt.minute // bucket_minutes) * bucket_minutes
            bucket = dt.replace(minute=minute, second=0, microsecond=0)
            key = bucket.strftime("%Y-%m-%d %H:%M")
            info = buckets.setdefault(key, {"requests": 0, "bytes": 0, "actors": set()})
            info["requests"] += 1
            info["bytes"] += r["bytes"]
            info["actors"].add(r["username"] if r["username"] != "-" else r["client_ip"])
        out = []
        for k in sorted(buckets.keys()):
            v = buckets[k]
            out.append((k, v["requests"], len(v["actors"]), v["bytes"]))
        return out

    def build_report(self):
        rows = self.source_rows()

        top_urls = {}
        top_users = {}
        top_domains = {}
        by_domain_bytes = {}
        for r in rows:
            top_urls[r["url"]] = top_urls.get(r["url"], 0) + 1
            key = r["username"] if r["username"] and r["username"] != "-" else r["client_ip"]
            top_users[key] = top_users.get(key, 0) + 1
            top_domains[r["domain"]] = top_domains.get(r["domain"], 0) + 1
            by_domain_bytes[r["domain"]] = by_domain_bytes.get(r["domain"], 0) + r["bytes"]

        top_urls_rows = sorted(top_urls.items(), key=lambda x: x[1], reverse=True)[:25]
        top_users_rows = sorted(top_users.items(), key=lambda x: x[1], reverse=True)[:25]
        top_domains_rows = [(dom, cnt, by_domain_bytes.get(dom, 0)) for dom, cnt in sorted(top_domains.items(), key=lambda x: x[1], reverse=True)[:25]]
        activity_rows = sorted(rows, key=lambda x: x["timestamp"], reverse=True)[:40]
        behavior_rows = self.behavior_hits(rows)
        trend_rows = self.trend_rows(rows)

        report_lines = []
        report_lines.append(f"Zeitraum: letzte {self.spin_minutes.value()} Minuten")
        report_lines.append(f"Requests gesamt: {len(rows)}")
        report_lines.append(f"Eindeutige Benutzer/IPs: {len(top_users)}")
        report_lines.append(f"Eindeutige URLs: {len(top_urls)}")
        report_lines.append("")
        report_lines.append("Welche URL wurde wann und von wem angewählt?")
        for x in activity_rows[:20]:
            report_lines.append(f"- {x['timestamp'].strftime('%Y-%m-%d %H:%M:%S')} | {x['username']} | {x['url']}")
        if behavior_rows:
            report_lines.append("")
            report_lines.append("Auffällige Verhaltensmuster:")
            for b in behavior_rows[:12]:
                report_lines.append(f"- {b[0]} | Kategorie={b[1]} | Treffer={b[6]} | {b[7]}")
        report_text = "".join(report_lines)

        return {
            "top_urls": top_urls_rows,
            "top_users": top_users_rows,
            "top_domains": top_domains_rows,
            "latest_rows": [
                (
                    x["timestamp"].strftime("%Y-%m-%d %H:%M:%S"), x["username"], x["client_ip"], x["method"], x["domain"], x["url"], x["bytes"], x["result_code"]
                ) for x in activity_rows
            ],
            "behavior_rows": behavior_rows,
            "trend_rows": trend_rows,
            "report_text": report_text,
            "generated_at": now_iso(),
            "range_minutes": self.spin_minutes.value(),
            "bucket_minutes": int(self.cb_bucket.currentText()),
        }

    def load(self):
        data = self.build_report()
        self.last_report_data = data
        self.fill_table(self.tbl_top_urls, ["URL", "Anzahl"], data["top_urls"])
        self.fill_table(self.tbl_top_users, ["Benutzer/IP", "Anzahl"], data["top_users"])
        self.fill_table(self.tbl_top_domains, ["Domain", "Anzahl", "Bytes"], data["top_domains"])
        self.fill_table(self.tbl_activity, ["Zeit", "Benutzer", "IP", "Methode", "Domain", "URL", "Bytes", "Result"], data["latest_rows"])
        self.fill_table(self.tbl_behavior, ["Regel", "Kategorie", "Scope", "Wert", "Fenster", "Threshold", "Treffer", "Akteure"], data["behavior_rows"])
        self.fill_table(self.tbl_trend, ["Bucket", "Requests", "Akteure", "Bytes"], data["trend_rows"])
        self.report.setPlainText(data["report_text"])

    def current_chart_data(self):
        if not self.last_report_data:
            self.load()
        choice = self.cb_chart.currentText()
        if choice == "Top Benutzer":
            rows = self.last_report_data["top_users"]
            labels = [str(r[0])[:40] for r in rows]
            values = [int(r[1]) for r in rows]
        elif choice == "Top Domains":
            rows = self.last_report_data["top_domains"]
            labels = [str(r[0])[:40] for r in rows]
            values = [int(r[1]) for r in rows]
        else:
            rows = self.last_report_data["top_urls"]
            labels = [str(r[0])[:40] for r in rows]
            values = [int(r[1]) for r in rows]
        return choice, labels, values

    def draw_bar_chart(self):
        title, labels, values = self.current_chart_data()
        self.chart.clear()
        ax = self.chart.ax
        if not values:
            ax.text(0.5, 0.5, "Keine Daten", ha="center", va="center")
        else:
            y_pos = list(range(len(labels)))
            ax.barh(y_pos, values)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(labels, fontsize=8)
            ax.invert_yaxis()
            ax.set_xlabel("Anzahl")
            ax.set_title(f"Balken-Diagramm: {title}")
        self.chart.draw()

    def draw_pie_chart(self):
        title, labels, values = self.current_chart_data()
        self.chart.clear()
        ax = self.chart.ax
        if not values:
            ax.text(0.5, 0.5, "Keine Daten", ha="center", va="center")
        else:
            ax.pie(values, labels=labels, autopct="%1.1f%%", textprops={"fontsize": 8})
            ax.set_title(f"Kreis-Diagramm: {title}")
        self.chart.draw()

    def export_csv_bundle(self):
        if not self.last_report_data:
            self.load()
        out_dir = Path(DB.setting("report_output_dir", str(APP_DIR / "reports")))
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        mapping = {
            "top_urls": ("top_urls", ["URL", "Anzahl"]),
            "top_users": ("top_users", ["Benutzer/IP", "Anzahl"]),
            "top_domains": ("top_domains", ["Domain", "Anzahl", "Bytes"]),
            "latest_rows": ("latest_activity", ["Zeit", "Benutzer", "IP", "Methode", "Domain", "URL", "Bytes", "Result"]),
            "behavior_rows": ("behavior_hits", ["Regel", "Kategorie", "Scope", "Wert", "Fenster", "Threshold", "Treffer", "Akteure"]),
            "trend_rows": ("trend", ["Bucket", "Requests", "Akteure", "Bytes"]),
        }

        written = []
        for key, (name, headers) in mapping.items():
            fp = out_dir / f"{stamp}_{name}.csv"
            with open(fp, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(headers)
                for row in self.last_report_data[key]:
                    w.writerow(row)
            written.append(str(fp))

        self.message("CSV Export", "Dateien geschrieben:" + "".join(written))

    def export_html_report(self):
        if not self.last_report_data:
            self.load()
        out_dir = Path(DB.setting("report_output_dir", str(APP_DIR / "reports")))
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fp = out_dir / f"{stamp}_proxy_report.html"

        html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Squid Report</title>
<style>
body {{ font-family: Segoe UI, Arial, sans-serif; margin: 24px; }}
table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; font-size: 12px; }}
th {{ background: #f0f0f0; }}
pre {{ background: #f7f7f7; padding: 12px; border: 1px solid #ddd; }}
h1, h2 {{ margin-top: 28px; }}
.small {{ color: #666; }}
</style>
</head>
<body>
<h1>Squid Proxy Report</h1>
<p class="small">Erzeugt: {escape(self.last_report_data['generated_at'])} | Zeitraum: {self.last_report_data['range_minutes']} Minuten | Trend-Bucket: {self.last_report_data['bucket_minutes']} Minuten</p>

<h2>Kurzreport</h2>
<pre>{escape(self.last_report_data['report_text'])}</pre>

<h2>Top URLs</h2>
{html_table(["URL", "Anzahl"], self.last_report_data["top_urls"])}

<h2>Top Benutzer</h2>
{html_table(["Benutzer/IP", "Anzahl"], self.last_report_data["top_users"])}

<h2>Top Domains</h2>
{html_table(["Domain", "Anzahl", "Bytes"], self.last_report_data["top_domains"])}

<h2>Letzte Aktivitäten</h2>
{html_table(["Zeit", "Benutzer", "IP", "Methode", "Domain", "URL", "Bytes", "Result"], self.last_report_data["latest_rows"])}

<h2>Verhaltensmuster Treffer</h2>
{html_table(["Regel", "Kategorie", "Scope", "Wert", "Fenster", "Threshold", "Treffer", "Akteure"], self.last_report_data["behavior_rows"])}

<h2>Trend</h2>
{html_table(["Bucket", "Requests", "Akteure", "Bytes"], self.last_report_data["trend_rows"])}
</body>
</html>
"""
        fp.write_text(html, encoding="utf-8")
        self.message("HTML Report", f"Report geschrieben:{fp}")

class ConfigTab(BaseCrudTab):
    def __init__(self):
        super().__init__()
        root = QVBoxLayout()
        form = QFormLayout()

        self.ed_access_log = QLineEdit(DB.setting("access_log_path", str(DEFAULT_ACCESS_LOG)))
        self.ed_cache_log = QLineEdit(DB.setting("cache_log_path", str(DEFAULT_CACHE_LOG)))
        self.ed_service = QLineEdit(DB.setting("squid_service_name", "Squid"))
        self.ed_binary = QLineEdit(DB.setting("squid_binary", "squid"))
        self.ed_conf = QLineEdit(DB.setting("squid_conf_path", str(DEFAULT_SQUID_CONF)))
        self.ed_python = QLineEdit(DB.setting("python_exe_path", "C:/Python311/python.exe"))
        self.ed_report_dir = QLineEdit(DB.setting("report_output_dir", str(APP_DIR / "reports")))
        self.spin_autosave = QSpinBox()
        self.spin_autosave.setRange(1, 1440)
        self.spin_autosave.setValue(int(DB.setting("autosave_minutes", "15") or "15"))

        def browse_file(target, title, filt="Alle Dateien (*)"):
            path, _ = QFileDialog.getOpenFileName(self, title, str(APP_DIR), filt)
            if path:
                target.setText(path)

        def browse_dir(target, title):
            path = QFileDialog.getExistingDirectory(self, title, str(APP_DIR))
            if path:
                target.setText(path)

        def browse_widget(line_edit, title, filt="Alle Dateien (*)", is_dir=False):
            row = QHBoxLayout()
            row.addWidget(line_edit)
            b = QPushButton("...")
            if is_dir:
                b.clicked.connect(lambda: browse_dir(line_edit, title))
            else:
                b.clicked.connect(lambda: browse_file(line_edit, title, filt))
            row.addWidget(b)
            w = QWidget()
            w.setLayout(row)
            return w

        form.addRow("squid.conf", browse_widget(self.ed_conf, "squid.conf auswählen", "Squid Config (squid.conf);;Alle Dateien (*)"))
        form.addRow("Python.exe", browse_widget(self.ed_python, "Python.exe auswählen", "Python (python.exe);;Ausführbare Dateien (*.exe);;Alle Dateien (*)"))
        form.addRow("Access Log", browse_widget(self.ed_access_log, "access.log auswählen"))
        form.addRow("Cache Log", browse_widget(self.ed_cache_log, "cache.log auswählen"))
        form.addRow("Report-Ordner", browse_widget(self.ed_report_dir, "Report-Ordner wählen", is_dir=True))
        form.addRow("Service Name", self.ed_service)
        form.addRow("Squid Binary", self.ed_binary)
        form.addRow("Autosave (Minuten)", self.spin_autosave)
        root.addLayout(form)

        buttons = QHBoxLayout()
        for text, fn in [("Einstellungen speichern", self.save), ("squid.conf generieren", self.generate_config), ("Konfiguration testen", self.test_config)]:
            b = QPushButton(text)
            b.clicked.connect(fn)
            buttons.addWidget(b)
        buttons.addStretch(1)
        root.addLayout(buttons)

        self.text = QTextEdit()
        root.addWidget(self.text)
        self.setLayout(root)

        conf_path = Path(self.ed_conf.text().strip())
        if conf_path.exists():
            self.text.setPlainText(conf_path.read_text(encoding="utf-8", errors="ignore"))

    def save(self):
        DB.set_setting("access_log_path", self.ed_access_log.text().strip())
        DB.set_setting("cache_log_path", self.ed_cache_log.text().strip())
        DB.set_setting("squid_service_name", self.ed_service.text().strip())
        DB.set_setting("squid_binary", self.ed_binary.text().strip())
        DB.set_setting("squid_conf_path", self.ed_conf.text().strip())
        DB.set_setting("python_exe_path", self.ed_python.text().strip())
        DB.set_setting("report_output_dir", self.ed_report_dir.text().strip())
        DB.set_setting("autosave_minutes", str(self.spin_autosave.value()))
        DB.set_setting("last_autosave", now_iso())
        self.message("Einstellungen", "Gespeichert.")

    def generate_config(self):
        self.save()
        conf_path = Path(self.ed_conf.text().strip() or str(DEFAULT_SQUID_CONF))
        py = self.ed_python.text().strip() or "C:/Python311/python.exe"
        authp = (APP_DIR / "basic_db_auth.py").as_posix()
        aclp = (APP_DIR / "db_acl_helper.py").as_posix()
        blocked = BLOCKED_FILE.as_posix()

        lines = [
            "http_port 3128",
            "visible_hostname squid-control-center-v5",
            "",
            f"access_log stdio:{self.ed_access_log.text().strip()}",
            f"cache_log stdio:{self.ed_cache_log.text().strip()}",
            "",
            f'auth_param basic program "{py}" "{authp}" "{DB_PATH.as_posix()}"',
            "auth_param basic realm Firmenproxy",
            "auth_param basic credentialsttl 2 hours",
            "auth_param basic casesensitive off",
            "acl authenticated proxy_auth REQUIRED",
            "",
            f'external_acl_type db_acl ttl=60 negative_ttl=20 %LOGIN %SRC %URI "{py}" "{aclp}" "{DB_PATH.as_posix()}"',
            "acl db_ok external db_acl",
            "",
            "acl localhost src 127.0.0.1/32 ::1",
            "acl localnet src 192.168.0.0/16",
            "acl localnet src 10.0.0.0/8",
            "acl SSL_ports port 443",
            "acl Safe_ports port 80",
            "acl Safe_ports port 443",
            "acl Safe_ports port 21",
            "acl Safe_ports port 1025-65535",
            "acl CONNECT method CONNECT",
            f'acl blocked_urls url_regex -i "{blocked}"',
            "",
            "# deny_info Vorlagen für Ersatz-Pages",
        ]

        rows = DB.fetchall("""
            SELECT DISTINCT b.category, COALESCE(r.file_path, '')
              FROM blocked_urls b
         LEFT JOIN replacement_pages r ON r.id=b.replacement_page_id
             WHERE b.is_enabled=1
          ORDER BY b.category
        """)
        for category, file_path in rows:
            safe = re.sub(r"[^A-Za-z0-9_]+", "_", category or "sonstiges")
            lines.append(f"# acl cat_{safe} url_regex -i \"{blocked}\"")
            if file_path:
                lines.append(f"# deny_info {Path(file_path).as_posix()} cat_{safe}")
            else:
                lines.append(f"# deny_info ERR_ACCESS_DENIED cat_{safe}")

        lines += [
            "",
            "http_access deny !Safe_ports",
            "http_access deny CONNECT !SSL_ports",
            "http_access allow localhost",
            "http_access deny !authenticated",
            "http_access deny !db_ok",
            "http_access deny blocked_urls",
            "http_access allow localnet authenticated db_ok",
            "http_access deny all",
        ]
        conf_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.text.setPlainText("\n".join(lines))
        self.message("Konfiguration", f"{conf_path} wurde erzeugt.")
    
    def test_config(self):
        try:
            if os.name == "nt":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= startupinfo.STARTF_USESHOWWINDOW
                
                p = subprocess.Popen(
                    [self.ed_binary.text().strip() or "squid", "-k", "parse", "-f", self.ed_conf.text().strip()],
                    capture_output = True,
                    text           = True,
                    creationflags  = subprocess.CREATE_NO_WINDOW,
                    startupinfo    = startupinfo,
                    stdout         = subprocess.PIPE,
                    stderr         = subprocess.PIPE
                )
                out, err = p.communicate(timeout=60)
                if p.returncode != 0:
                    raise RuntimeError(f"Squid failed ({p.returncode}):\n{err}")
                self.message("Konfigurationstest", result.stdout + "\n" + result.stderr)
                return True
            else:
                content = "Squid is only available under Microsoft Windows"
                dlg = ErrorMessage(
                    title    = "Laufzeitfehler",
                    message  = content,
                    log_path = LOG,
                    parent   = MAINWIN
                )
                dlg.exec_()
                return False
        except Exception as e:
            self.warn("Fehler", str(e))
            return False

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Squid Control Center v8")
        self.resize(1024, 720)

        tabs = QTabWidget()
        tabs.addTab(DashboardTab()        , "Dashboard")
        tabs.addTab(UrlFilterTab()        , "URL Filter")
        tabs.addTab(ReplacementPagesTab() , "Ersatz-Pages")
        tabs.addTab(UsersTab()            , "Benutzer")
        tabs.addTab(GroupsTab()           , "Gruppen")
        tabs.addTab(NetworksTab()         , "Netzwerke")
        tabs.addTab(TimeWindowsTab()      , "Zeitfenster")
        tabs.addTab(BehaviorRulesTab()    , "Verhaltensmuster")
        tabs.addTab(LiveClientsTab()      , "Live Clients")
        tabs.addTab(LogsTab()             , "Logs")
        tabs.addTab(StatisticsTab()       , "Statistiken")
        tabs.addTab(ConfigTab()           , "Konfiguration")
        self.setCentralWidget(tabs)

        self.f1filter = F1Filter(self)
        QApplication.instance().installEventFilter(self.f1filter)
        AppMode.dark = True
        self._apply_theme()
        
        self.autosave_timer = QTimer(self)
        self.autosave_timer.timeout.connect(self.autosave_everything)
        self.restart_autosave_timer()
        
        self._create_menubar()
        
    def _create_menubar(self):
        menubar = self.menuBar()

        # Schrift setzen
        menubar.setFont(QFont("Arial", 11))

        # Style setzen
        menubar.setStyleSheet(f"""
        QMenuBar {{
            background-color: navy;
            color: yellow;
            font: 11pt "Arial";
        }}
        QMenuBar::item {{
            background-color: transparent;
            color: yellow;
            padding: 6px 12px;
        }}
        QMenuBar::item:selected {{
            background-color: #1e3a5f;
        }}
        QMenu {{
            background-color: navy;
            color: yellow;
            border: 1px solid #3f5f8f;
            font: 11pt "Arial";
        }}
        QMenu::item {{
            padding: 6px 24px 6px 24px;
            background-color: transparent;
            color: yellow;
        }}
        QMenu::item:selected {{
            background-color: #1e3a5f;
        }}
        """)

        # Menü "Datei"
        menu_datei = menubar.addMenu("Datei")

        action_beenden = QAction("Beenden", self)
        action_beenden.triggered.connect(self.close)
        menu_datei.addAction(action_beenden)

        # Menü "Hilfe"
        menu_hilfe = menubar.addMenu("Hilfe")

        action_ueber = QAction("Über", self)
        action_ueber.triggered.connect(self.show_about)
        menu_hilfe.addAction(action_ueber)

    def show_about(self):
        QMessageBox.about(
            self,
            "Über",
            "Dies ist ein Beispiel für eine Menüleiste mit Untermenüs."
        )

    def restart_autosave_timer(self):
        minutes = int(DB.setting("autosave_minutes", "15") or "15")
        self.autosave_timer.start(minutes * 60 * 1000)

    def autosave_everything(self):
        DB.set_setting("last_autosave", now_iso())
        config_tab = self.find_config_tab()
        if config_tab:
            config_tab.save()
        self.restart_autosave_timer()

    def find_config_tab(self):
        tabs = self.centralWidget()
        for i in range(tabs.count()):
            w = tabs.widget(i)
            if isinstance(w, ConfigTab):
                return w
        return None
    
    def _apply_theme(self):
        app = QApplication.instance()
        pal = QPalette()
        
        if AppMode.dark:
            pal.setColor(QPalette.Window, QColor(30, 30, 30))
            pal.setColor(QPalette.WindowText, Qt.white)
            pal.setColor(QPalette.Base, QColor(24, 24, 24))
            pal.setColor(QPalette.AlternateBase, QColor(35, 35, 35))
            pal.setColor(QPalette.Text, Qt.white)
            pal.setColor(QPalette.Button, QColor(45, 45, 45))
            pal.setColor(QPalette.ButtonText, Qt.white)
            pal.setColor(QPalette.Highlight, QColor(80, 120, 200))
            pal.setColor(QPalette.HighlightedText, Qt.white)
        else:
            pal = app.style().standardPalette()
        
        app.setPalette(pal)
        
        if AppMode.dark:
            AppMode.dark = True
            header_bg               = "#222222"
            header_fg               = "#ffff00"
            tree_bg                 = "#181818"
            tree_fg                 = "#ffffff"
            sel_bg                  = "#2b4c7e"
            sel_fg                  = "#ffffff"
            border                  = "#333333"
            
            tab_bg                  = "#1c1c1c"
            tab_bar_bg              = "#161616"
            tab_fg                  = "#eaeaea"
            tab_fg_active           = "#ffd866"
            tab_sel_bg              = "#242424"
            tab_hover_bg            = "#202020"
            
            toolbar_bg              = "#1a1a1a"
            toolbtn_bg              = "#222222"
            toolbtn_fg              = "#ffd866"
            toolbtn_hover           = "#2a2a2a"
            toolbtn_pressed         = "#303030"
            
            title_bg                = "#121212"  # Hintergrund Titelleiste
            title_fg                = "#ffd866"  # Text/Farbe Buttons (oder "#ffffff")
            title_btn_bg            = "#1f1f1f"  # Buttons normal
            title_btn_hover         = "#2a2a2a"  # Buttons hover
            title_btn_close_hover   = "#8a1f1f"  # Close hover
            
            status_bg               = "#121212"
            status_fg               = "#ffd866"  # oder "#ffffff"
            status_border           = "#333333"
            
            # Scrollbar dark-blue
            scroll_track            = "#141414"
            scroll_handle           = "#0b2a4a"
            scroll_handle_hover     = "#0f3a66"
        else:
            AppMode.dark = False
            header_bg               = "#f0f0f0"
            header_fg               = "#000000"
            tree_bg                 = "#ffffff"
            tree_fg                 = "#000000"
            sel_bg                  = "#cfe3ff"
            sel_fg                  = "#000000"
            border                  = "#d0d0d0"
            
            tab_bg                  = "#f4f4f4"
            tab_bar_bg              = "#ededed"
            tab_fg                  = "#000000"
            tab_fg_active           = "#000000"
            tab_sel_bg              = "#ffffff"
            tab_hover_bg            = "#f9f9f9"
            
            toolbar_bg              = "#f2f2f2"
            toolbtn_bg              = "#e9e9e9"
            toolbtn_fg              = "#000000"
            toolbtn_hover           = "#dedede"
            toolbtn_pressed         = "#d2d2d2"
            
            title_bg                = "#eaeaea"
            title_fg                = "#000000"
            title_btn_bg            = "#f3f3f3"
            title_btn_hover         = "#dedede"
            title_btn_close_hover   = "#e06c75"
            
            status_bg               = "#ededed"
            status_fg               = "#000000"
            status_border           = "#d0d0d0"
            
            # Scrollbar light-gray
            scroll_track            = "#f2f2f2"
            scroll_handle           = "#c8c8c8"
            scroll_handle_hover     = "#b0b0b0"
        
        #self.setStyleSheet(_css("default_dark"))
        self.setStyleSheet("""
QToolBar {{spacing: 8px;background: {toolbar_bg};border: none;}}
QToolBar::separator {{background: {border};width: 1px;margin: 6px 8px;}}
QLineEdit {{padding: 6px 10px;border-radius: 10px;border: 1px solid {border};background: {tab_bg};color: {tab_fg};}}
QLabel {{color: {tab_fg};}}
QToolButton {{background: {toolbtn_bg};color: {toolbtn_fg};border: 1px solid {border};border-radius: 10px;padding: 6px 10px;}}
QToolButton:hover {{background: {toolbtn_hover};}}
QToolButton:pressed {{background: {toolbtn_pressed};}}
QTabWidget::pane {{border: 1px solid {border};top: -1px;background: {tab_bg};}}
QTabBar {{background: {tab_bar_bg};}}
QTabBar::tab {{background: {tab_bar_bg};color: {tab_fg};border: 1px solid {border};border-bottom: none;padding: 7px 14px;margin-right: 6px;border-top-left-radius: 12px;border-top-right-radius: 12px;min-width: 90px;}}
QTabBar::tab:hover {{background: {tab_hover_bg};}}
QTabBar::tab:selected {{background: {tab_sel_bg};color: {tab_fg_active};}}
QTreeView {{border: none;background: {tree_bg};color: {tree_fg};}}
QTreeView::item:selected {{background: {sel_bg};color: {sel_fg};}}
QHeaderView::section {{background: {header_bg};color: {header_fg};padding: 6px;border: none;border-bottom: 1px solid {border};}}
QPushButton {{background: {toolbtn_bg};color: {toolbtn_fg};border: 1px solid {border};border-radius: 10px;padding: 7px 12px;}}
QPushButton:hover {{background: {toolbtn_hover};}}
QPushButton:pressed {{background: {toolbtn_pressed};}}
/* Scrollbars (TreeView etc.) */
QScrollBar:vertical {{background: {scroll_track};width: 12px;margin: 0px;border: none;border-radius: 6px;}}
QScrollBar::handle:vertical {{background: {scroll_handle};min-height: 28px;border-radius: 6px;}}
QScrollBar::handle:vertical:hover {{background: {scroll_handle_hover};}}
QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical {{height: 0px;}}
QScrollBar::add-page:vertical,QScrollBar::sub-page:vertical {{background: transparent;}}
QScrollBar:horizontal {{background: {scroll_track};height: 12px;margin: 0px;border: none;border-radius: 6px;}}
QScrollBar::handle:horizontal {{background: {scroll_handle};min-width: 28px;border-radius: 6px;}}
QScrollBar::handle:horizontal:hover {{background: {scroll_handle_hover};}}
QScrollBar::add-line:horizontal,QScrollBar::sub-line:horizontal {{width: 0px;}}
QScrollBar::add-page:horizontal,QScrollBar::sub-page:horizontal {{background: transparent;}}
/* Custom Title Bar */
#TopContainer {{ background: transparent; }}
#TitleLabel {{color: {title_fg};font-weight: 600;}}
#TitleSeparator {{background: {border};}}
QPushButton#TitleBtnMin,QPushButton#TitleBtnMax,QPushButton#TitleBtnClose {{background: {title_btn_bg};color: {title_fg};border: 1px solid {border};border-radius: 10px;}}
QPushButton#TitleBtnMin:hover,QPushButton#TitleBtnMax:hover {{background: {title_btn_hover};}}
QPushButton#TitleBtnClose:hover {{background: {title_btn_close_hover};}}
QStatusBar {{background: {status_bg};color: {status_fg};border-top: 1px solid {status_border};}}
QStatusBar QLabel {{color: {status_fg};}}
/* Tab scrollers (left/right arrows) */
QTabBar::scroller {{width: 22px;height: 22px;background: {tab_bar_bg};border: 1px solid {border};border-radius: 10px;margin: 2px;}}
QTabBar::scroller:hover {{background: {tab_hover_bg};}}
/* Arrow icons color via "color" + qproperty (works in many styles) */
QTabBar QToolButton {{background: {tab_bar_bg};border: 1px solid {border};border-radius: 10px;padding: 2px;color: {tab_fg_active};}}
QTabBar QToolButton:hover {{background: {tab_hover_bg};}}
QTabBar QToolButton:pressed {{background: {tab_sel_bg};}}
QSplitter {{background: {tree_bg};}}
QSplitter::handle {{background: {border};}}
QWebEngineView {{background: {tree_bg};}}
""")

def ensure_demo_data():
    if not DB.fetchall("SELECT id FROM groups LIMIT 1"):
        DB.execute("INSERT INTO groups(name, is_enabled, comment) VALUES('Mitarbeiter', 1, 'Standardgruppe')")
        DB.execute("INSERT INTO groups(name, is_enabled, comment) VALUES('Gäste', 1, 'Gastzugänge')")

    if not DB.fetchall("SELECT id FROM networks LIMIT 1"):
        DB.execute("INSERT INTO networks(name, cidr, is_enabled, comment) VALUES('LAN', '192.168.0.0/24', 1, 'Intern')")
        DB.execute("INSERT INTO networks(name, cidr, is_enabled, comment) VALUES('VPN', '10.8.0.0/24', 1, 'VPN')")

    if not DB.fetchall("SELECT id FROM time_windows LIMIT 1"):
        DB.execute("INSERT INTO time_windows(name, weekdays, start_time, end_time, is_enabled, comment) VALUES('Arbeitszeit', 'mon,tue,wed,thu,fri', '08:00', '18:00', 1, 'Standard')")

    if not DB.fetchall("SELECT id FROM users LIMIT 1"):
        gid = DB.fetchall("SELECT id FROM groups WHERE name='Mitarbeiter'")[0][0]
        nid = DB.fetchall("SELECT id FROM networks WHERE name='LAN'")[0][0]
        tid = DB.fetchall("SELECT id FROM time_windows WHERE name='Arbeitszeit'")[0][0]
        DB.execute("""
            INSERT INTO users(username, password_hash, cert_fingerprint, is_enabled, is_blocked, group_id, network_id, time_window_id)
            VALUES(?,?,?,?,?,?,?,?)
        """, ("admin", hash_password("admin"), "", 1, 0, gid, nid, tid))

    template = APP_DIR / "blocked_template.html"
    if not template.exists():
        template.write_text("<html><body><h1>Zugriff blockiert</h1><p>Diese Seite wurde blockiert.</p></body></html>", encoding="utf-8")

    if not DB.fetchall("SELECT id FROM replacement_pages LIMIT 1"):
        DB.execute("INSERT INTO replacement_pages(name, category, file_path, is_enabled, comment) VALUES('Standard Blockseite', 'sonstiges', ?, 1, 'Demo')", (str(template),))

    if not DB.fetchall("SELECT id FROM blocked_urls LIMIT 1"):
        rid = DB.fetchall("SELECT id FROM replacement_pages LIMIT 1")[0][0]
        DB.execute("INSERT INTO blocked_urls(pattern, category, is_regex, is_enabled, replacement_page_id, comment) VALUES('facebook.com', 'FSK', 0, 1, ?, 'Demo')", (rid,))
        DB.execute("INSERT INTO blocked_urls(pattern, category, is_regex, is_enabled, replacement_page_id, comment) VALUES('casino', 'Glücksspiel', 0, 1, ?, 'Demo')", (rid,))

    if not DB.fetchall("SELECT id FROM behavior_rules LIMIT 1"):
        DB.execute("""
            INSERT INTO behavior_rules(name, url_pattern, category, is_regex, scope_type, scope_value, window_minutes, threshold_count, is_enabled, comment)
            VALUES('Häufige Social-Media-Aufrufe', 'facebook.com', 'FSK', 0, 'all', '', 60, 3, 1, 'Demo-Regel')
        """)

    if not DEFAULT_ACCESS_LOG.exists():
        sample = [
            "1712345678.123 200 192.168.0.10 TCP_MISS/200 1024 GET http://facebook.com admin DIRECT/93.184.216.34 text/html",
            "1712345690.500 180 192.168.0.11 TCP_MISS/200 2048 GET http://casino.example guest DIRECT/192.168.0.2 text/html",
            "1712345701.210 220 192.168.0.10 TCP_MISS/200 4096 GET http://news.example.org/article admin DIRECT/198.51.100.5 text/html",
            "1712345710.210 220 192.168.0.10 TCP_MISS/200 4096 GET http://facebook.com/profile admin DIRECT/198.51.100.6 text/html",
            "1712345720.210 220 192.168.0.10 TCP_MISS/200 4096 GET http://facebook.com/messages admin DIRECT/198.51.100.7 text/html",
            "1712345730.210 240 192.168.0.15 TCP_MISS/200 1024 GET http://example.org helpdesk DIRECT/198.51.100.8 text/html",
        ]
        DEFAULT_ACCESS_LOG.write_text("\n".join(sample) + "\n", encoding="utf-8")

    if not DEFAULT_CACHE_LOG.exists():
        DEFAULT_CACHE_LOG.write_text("cache.log Beispiel\n", encoding="utf-8")

def main():
    init_db()
    ensure_demo_data()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    global MAINWIN
    MAINWIN = MainWindow()
    MAINWIN.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
