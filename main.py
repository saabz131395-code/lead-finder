import sys
import os
import sqlite3
import json
import re
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional

from PySide6.QtCore import Qt, QThread, Signal, QObject, Slot, QTimer
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QComboBox, QPushButton, QSlider, QCheckBox,
    QTableWidget, QTableWidgetItem, QMessageBox, QFrame, QSplitter,
    QHeaderView, QProgressBar, QFileDialog, QTabWidget, QFormLayout,
    QSpinBox, QScrollArea
)
from PySide6.QtGui import QShortcut, QKeySequence

from sources import SourceManager
from engine.cache import SeenLeadCache

APP_NAME = "USA Lead Finder"
def get_db_path():
    """Store history in a writable persistent user folder (works with PyInstaller one-file)."""
    if sys.platform.startswith("win"):
        base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    else:
        base = os.path.join(os.path.expanduser("~"), ".local", "share")
    app_dir = os.path.join(base, "USA Lead Finder")
    os.makedirs(app_dir, exist_ok=True)
    return os.path.join(app_dir, "lead_history.db")

DB_PATH = get_db_path()
CHECKPOINT_PATH = os.path.join(
    os.path.dirname(DB_PATH),
    "active_search_checkpoint.json",
)

CLEAN_EXIT_MARKER_PATH = os.path.join(
    os.path.dirname(DB_PATH),
    "clean_exit.marker",
)

KEYWORDS = [
    "Plumber", "Electrician", "HVAC", "Roofer", "Dentist", "Doctor",
    "Chiropractor", "Lawyer", "Accountant", "Real Estate Agent",
    "Insurance Agent", "Auto Repair", "Towing", "Locksmith", "Pest Control",
    "Landscaping", "Cleaning Service", "Painting Contractor", "General Contractor",
    "Remodeling", "Solar Installer", "Restaurant", "Gym", "Salon",
    "Veterinarian", "Moving Company", "Photographer", "Web Design",
    "Digital Marketing", "SEO Agency"
]

SOURCES = ["Google Maps", "Hotfrog", "Yellow Pages", "LinkedIn"]

@dataclass
class Lead:
    name: str
    keyword: str
    city: str = ""
    state: str = ""
    country: str = "USA"

    phone: str = ""
    website: str = ""
    email: str = ""
    address: str = ""

    rating: str = ""
    reviews: str = ""

    sources: str = ""
    source: str = ""

    linkedin: str = ""
    facebook: str = ""
    instagram: str = ""

    notes: str = ""
    source_links: object = ""

    zip_code: str = ""

    google_maps_url: str = ""

    latitude: str = ""
    longitude: str = ""


class Database:
    """Small SQLite wrapper that opens a fresh connection per operation.

    This avoids sharing one SQLite connection across Qt worker/UI threads.
    """
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._initialize()

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _initialize(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS searches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    country TEXT,
                    city TEXT,
                    state TEXT,
                    zip TEXT,
                    target_leads INTEGER,
                    sources TEXT,
                    result_count INTEGER,
                    results_json TEXT
                )
                """
            )

    def save_search(self, params: Dict, leads: List[Lead]):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO searches (
                    created_at, keyword, country, city, state, zip,
                    target_leads, sources, result_count, results_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now().isoformat(timespec="seconds"),
                    params["keyword"], params["country"], params["city"],
                    params["state"], params["zip"], params["target_leads"],
                    json.dumps(params["sources"]), len(leads),
                    json.dumps([asdict(x) for x in leads], ensure_ascii=False),
                )
            )

    def history(self):
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT id, created_at, keyword, country, city, state, target_leads, sources, result_count, results_json "
                "FROM searches ORDER BY id DESC"
            )
            return cur.fetchall()


class SearchWorker(QObject):
    progress = Signal(int, str)
    source_progress = Signal(str, int)
    lead_found = Signal(dict)
    finished = Signal(list, dict)
    error = Signal(str)
    cancelled = Signal(list, dict)

    def __init__(self, params: Dict):
        super().__init__()
        self.params = params
        self._cancel_requested = False
        self._paused = False
        self.manager = None

    def cancel(self):
        self._cancel_requested = True
        self._paused = False
        if self.manager:
            self.manager.cancel()

    def pause(self):
        if self._cancel_requested:
            return
        self._paused = True
        if self.manager:
            self.manager.pause()

    def resume(self):
        if self._cancel_requested:
            return
        self._paused = False
        if self.manager:
            self.manager.resume()

    def is_cancelled(self):
        return self._cancel_requested

    def run(self):
        try:
            self.manager = SourceManager()

            if self._cancel_requested:
                self.manager.cancel()

            leads, source_counts, was_cancelled = self.manager.search(
                keyword=self.params["keyword"],
                country=self.params["country"],
                city=self.params["city"],
                state=self.params["state"],
                zip_code=self.params["zip"],
                target_leads=self.params["target_leads"],
                sources=self.params["sources"],
                progress_cb=self._progress,
                source_cb=self._source,
                lead_cb=self._lead,
                search_mode=self.params.get("search_mode", "Balanced"),
                skip_previous=self.params.get("skip_previous", True),
            )

            if was_cancelled:
                self.cancelled.emit(leads, source_counts)
            else:
                self.finished.emit(leads, source_counts)

        except Exception as exc:
            self.error.emit(str(exc))

    def _progress(self, value: int, text: str):
        self.progress.emit(value, text)

    def _source(self, source: str, count: int):
        self.source_progress.emit(source, count)

    def _lead(self, lead: dict):
        self.lead_found.emit(dict(lead))

class Card(QFrame):
    def __init__(self, title: str, value: str = "0"):
        super().__init__()
        self.setObjectName("card")
        lay = QVBoxLayout(self)
        t = QLabel(title)
        t.setObjectName("cardTitle")
        self.value = QLabel(value)
        self.value.setObjectName("cardValue")
        lay.addWidget(t)
        lay.addWidget(self.value)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db = Database()
        self.thread: Optional[QThread] = None
        self.worker: Optional[SearchWorker] = None
        self.current_leads: List[Lead] = []
        self.ui_scale = 1.0

        self._exit_after_stop = False
        self._intentional_exit = False
        self._saved_incomplete_search_on_exit = False
        self._resume_base_leads: List[Lead] = []
        self._resume_elapsed_offset = 0
        self._last_checkpoint_save = -1
        self._timeout_minutes = 0
        self._timeout_window_started = None

        self.setWindowTitle(APP_NAME)
        self.resize(1500, 900)
        self._build_ui()
        self._load_history()
        self._apply_style()
        self._refresh_cache_info()

        # If the last session exited cleanly, its marker may still exist.
        # Removing it at startup means a crash during THIS session leaves no
        # clean-exit marker and therefore preserves checkpoint/cache recovery.
        self._remove_clean_exit_marker()

        QTimer.singleShot(600, self._offer_resume_checkpoint)

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)
        main.setContentsMargins(24, 18, 24, 18)
        main.setSpacing(14)

        title_row = QHBoxLayout()
        title = QLabel("Lead Discovery Dashboard")
        title.setObjectName("pageTitle")
        subtitle = QLabel("Unified business leads from multiple sources")
        subtitle.setObjectName("subtitle")
        left_title = QVBoxLayout()
        left_title.addWidget(title)
        left_title.addWidget(subtitle)
        title_row.addLayout(left_title)
        title_row.addStretch()

        zoom_out = QPushButton("−")
        zoom_out.setObjectName("zoomButton")
        zoom_out.setFixedWidth(38)
        zoom_out.clicked.connect(lambda: self._change_zoom(-0.1))
        title_row.addWidget(zoom_out)

        self.zoom_label = QLabel("100%")
        self.zoom_label.setObjectName("zoomLabel")
        self.zoom_label.setAlignment(Qt.AlignCenter)
        self.zoom_label.setMinimumWidth(58)
        title_row.addWidget(self.zoom_label)

        zoom_in = QPushButton("+")
        zoom_in.setObjectName("zoomButton")
        zoom_in.setFixedWidth(38)
        zoom_in.clicked.connect(lambda: self._change_zoom(0.1))
        title_row.addWidget(zoom_in)

        self.zoom_in_shortcut = QShortcut(QKeySequence("Ctrl++"), self)
        self.zoom_in_shortcut.activated.connect(lambda: self._change_zoom(0.1))
        self.zoom_in_shortcut2 = QShortcut(QKeySequence("Ctrl+="), self)
        self.zoom_in_shortcut2.activated.connect(lambda: self._change_zoom(0.1))
        self.zoom_out_shortcut = QShortcut(QKeySequence("Ctrl+-"), self)
        self.zoom_out_shortcut.activated.connect(lambda: self._change_zoom(-0.1))
        self.zoom_reset_shortcut = QShortcut(QKeySequence("Ctrl+0"), self)
        self.zoom_reset_shortcut.activated.connect(self._reset_zoom)

        main.addLayout(title_row)

        search_box = QFrame()
        search_box.setObjectName("panel")
        s = QVBoxLayout(search_box)
        s.setContentsMargins(14, 12, 14, 12)
        s.setSpacing(10)

        # -------------------------------------------------
        # Row 1: primary search/location fields
        # -------------------------------------------------
        row1 = QHBoxLayout()
        row1.setSpacing(10)

        self.keyword = QComboBox()
        self.keyword.setEditable(True)
        self.keyword.addItems(KEYWORDS)
        self.keyword.setMinimumWidth(240)
        row1.addWidget(self._labeled("Keyword / Niche", self.keyword), 3)

        self.city = QLineEdit()
        self.city.setPlaceholderText("e.g. Austin")
        row1.addWidget(self._labeled("City", self.city), 3)

        self.state = QLineEdit()
        self.state.setPlaceholderText("e.g. TX")
        self.state.setMaximumWidth(120)
        row1.addWidget(self._labeled("State", self.state), 1)

        self.zip_code = QLineEdit()
        self.zip_code.setPlaceholderText("ZIP")
        self.zip_code.setMaximumWidth(110)
        row1.addWidget(self._labeled("ZIP", self.zip_code), 1)

        self.country = QComboBox()
        self.country.setEditable(True)
        self.country.addItems(["USA", ""])
        self.country.setCurrentText("USA")
        self.country.setMaximumWidth(115)
        row1.addWidget(self._labeled("Country", self.country), 1)

        self.current_location = QPushButton("Use Current Location")
        self.current_location.setObjectName("secondaryAction")
        self.current_location.setMinimumHeight(38)
        self.current_location.setMinimumWidth(155)
        self.current_location.clicked.connect(self._use_current_location)
        row1.addWidget(self.current_location)

        s.addLayout(row1)

        # -------------------------------------------------
        # Row 2: lead target + source selection only
        # -------------------------------------------------
        row2 = QHBoxLayout()
        row2.setSpacing(16)

        lead_panel = QFrame()
        lead_panel.setObjectName("miniPanel")
        lead_box = QVBoxLayout(lead_panel)
        lead_box.setContentsMargins(10, 7, 10, 7)
        lead_box.setSpacing(5)

        lead_top = QHBoxLayout()
        lead_top.addWidget(QLabel("Number of leads"))
        self.lead_value = QLabel("100")
        self.lead_value.setObjectName("accentText")
        lead_top.addStretch()
        lead_top.addWidget(self.lead_value)
        lead_box.addLayout(lead_top)

        self.lead_slider = QSlider(Qt.Horizontal)
        self.lead_slider.setRange(10, 500)
        self.lead_slider.setSingleStep(10)
        self.lead_slider.setPageStep(25)
        self.lead_slider.setValue(100)
        self.lead_slider.valueChanged.connect(
            lambda v: self.lead_value.setText(str(v))
        )
        lead_box.addWidget(self.lead_slider)
        row2.addWidget(lead_panel, 3)

        sources_panel = QFrame()
        sources_panel.setObjectName("miniPanel")
        sources_box = QVBoxLayout(sources_panel)
        sources_box.setContentsMargins(10, 7, 10, 7)
        sources_box.setSpacing(6)
        sources_box.addWidget(QLabel("Sources"))

        src_row = QHBoxLayout()
        src_row.setSpacing(16)
        self.source_checks = {}
        for source in SOURCES:
            cb = QCheckBox(source)
            cb.setChecked(True)
            self.source_checks[source] = cb
            src_row.addWidget(cb)

        src_row.addStretch()
        sources_box.addLayout(src_row)
        row2.addWidget(sources_panel, 5)

        s.addLayout(row2)

        # -------------------------------------------------
        # Row 3: mode/cache on left, action buttons on right
        # -------------------------------------------------
        row3 = QHBoxLayout()
        row3.setSpacing(10)

        mode_label = QLabel("Mode")
        mode_label.setObjectName("mutedLabel")
        row3.addWidget(mode_label)

        self.search_mode = QComboBox()
        self.search_mode.addItems(["Fast", "Balanced", "Deep"])
        self.search_mode.setCurrentText("Balanced")
        self.search_mode.setFixedWidth(125)
        row3.addWidget(self.search_mode)

        timeout_label = QLabel("Timeout")
        timeout_label.setObjectName("mutedLabel")
        row3.addWidget(timeout_label)

        self.timeout_combo = QComboBox()
        self.timeout_combo.addItems([
            "No Limit",
            "10 min",
            "20 min",
            "30 min",
            "60 min",
        ])
        self.timeout_combo.setCurrentText("No Limit")
        self.timeout_combo.setFixedWidth(105)
        row3.addWidget(self.timeout_combo)

        divider1 = QFrame()
        divider1.setFrameShape(QFrame.VLine)
        divider1.setObjectName("softDivider")
        row3.addWidget(divider1)

        self.skip_previous = QCheckBox("Skip previous leads")
        self.skip_previous.setChecked(True)
        row3.addWidget(self.skip_previous)

        self.cache_info = QLabel("Cache: 0 previous leads")
        self.cache_info.setObjectName("cacheBadge")
        row3.addWidget(self.cache_info)

        self.clear_cache_btn = QPushButton("Clear Cache")
        self.clear_cache_btn.setObjectName("compactButton")
        self.clear_cache_btn.setToolTip(
            "Clear cached leads only for the current keyword and location"
        )
        self.clear_cache_btn.clicked.connect(self._clear_current_search_cache)
        row3.addWidget(self.clear_cache_btn)

        row3.addStretch()

        self.search_btn = QPushButton("Search Leads")
        self.search_btn.setObjectName("primaryButton")
        self.search_btn.setMinimumHeight(40)
        self.search_btn.setMinimumWidth(125)
        self.search_btn.clicked.connect(self.start_search)
        row3.addWidget(self.search_btn)

        self.pause_btn = QPushButton("Pause")
        self.pause_btn.setMinimumHeight(40)
        self.pause_btn.setMinimumWidth(78)
        self.pause_btn.setEnabled(False)
        self.pause_btn.clicked.connect(self.pause_or_resume_search)
        row3.addWidget(self.pause_btn)

        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setObjectName("stopButton")
        self.stop_btn.setMinimumHeight(40)
        self.stop_btn.setMinimumWidth(72)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_search)
        row3.addWidget(self.stop_btn)

        s.addLayout(row3)
        main.addWidget(search_box)

        cards = QHBoxLayout()
        self.total_card = Card("Total Leads")
        cards.addWidget(self.total_card)
        self.source_cards = {}
        for source in SOURCES:
            c = Card(source)
            self.source_cards[source] = c
            cards.addWidget(c)
        main.addLayout(cards)

        progress_row = QHBoxLayout()
        progress_row.setSpacing(10)

        self.progress = QProgressBar()
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        progress_row.addWidget(self.progress, 1)

        self.time_label = QLabel("0 leads  •  00:00  •  -- sec/lead")
        self.time_label.setObjectName("timerBadge")
        self.time_label.setAlignment(Qt.AlignCenter)
        self.time_label.setMinimumWidth(250)
        progress_row.addWidget(self.time_label)

        main.addLayout(progress_row)

        self.search_timer = QTimer(self)
        self.search_timer.setInterval(1000)
        self.search_timer.timeout.connect(self._update_elapsed_display)
        self._search_started_at = None
        self._last_elapsed_seconds = 0

        self.tabs = QTabWidget()
        main.addWidget(self.tabs, 1)

        # Results tab: compact lead list with a closable side-by-side details panel.
        results_tab = QWidget()
        results_layout = QVBoxLayout(results_tab)
        top_actions = QHBoxLayout()
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("subtitle")
        top_actions.addWidget(self.status_label)
        top_actions.addStretch()
        export_btn = QPushButton("Export CSV")
        export_btn.clicked.connect(self.export_csv)
        top_actions.addWidget(export_btn)
        results_layout.addLayout(top_actions)

        self.results_splitter = QSplitter(Qt.Horizontal)
        self.results_splitter.setChildrenCollapsible(False)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels([
            "#", "Name", "Location", "Phone", "Sources", "Details"
        ])
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.table.itemSelectionChanged.connect(self._show_selected_lead)
        self.table.cellDoubleClicked.connect(lambda row, _col: self._open_lead_details(row))
        self.results_splitter.addWidget(self.table)

        # Browser-style details drawer on the right. Hidden until View Details is clicked.
        self.details_panel = QFrame()
        self.details_panel.setObjectName("detailsPanel")
        self.details_panel.setMinimumWidth(390)
        self.details_panel.setMaximumWidth(620)
        panel_layout = QVBoxLayout(self.details_panel)
        panel_layout.setContentsMargins(16, 14, 16, 16)
        panel_layout.setSpacing(10)

        details_top = QHBoxLayout()
        details_title = QLabel("Lead Details")
        details_title.setObjectName("detailsTitle")
        details_top.addWidget(details_title)
        details_top.addStretch()
        close_btn = QPushButton("✕")
        close_btn.setObjectName("closeDetailsButton")
        close_btn.setFixedSize(34, 34)
        close_btn.setToolTip("Close details")
        close_btn.clicked.connect(self._close_details_panel)
        details_top.addWidget(close_btn)
        panel_layout.addLayout(details_top)

        details_scroll = QScrollArea()
        details_scroll.setWidgetResizable(True)
        details_scroll.setFrameShape(QFrame.NoFrame)
        details_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        detail_body = QWidget()
        detail_lay = QFormLayout(detail_body)
        detail_lay.setContentsMargins(4, 6, 8, 12)
        detail_lay.setHorizontalSpacing(18)
        detail_lay.setVerticalSpacing(13)
        detail_lay.setLabelAlignment(Qt.AlignTop | Qt.AlignLeft)

        self.detail_labels = {}
        detail_keys = [
            "Name", "Phone", "Website", "Email", "Address",
            "Rating", "Reviews", "LinkedIn", "Notes", "Sources", "Source Links"
        ]
        for key in detail_keys:
            lbl = QLabel("-")
            lbl.setWordWrap(True)
            lbl.setTextInteractionFlags(Qt.TextBrowserInteraction)
            lbl.setOpenExternalLinks(True)
            lbl.setMinimumWidth(210)
            if key in ("Name", "Website", "LinkedIn", "Source Links"):
                lbl.setObjectName("detailValueImportant")
            self.detail_labels[key] = lbl

            field_name = QLabel(key + ":")
            field_name.setObjectName("detailFieldName")
            detail_lay.addRow(field_name, lbl)

        details_scroll.setWidget(detail_body)
        panel_layout.addWidget(details_scroll, 1)
        self.results_splitter.addWidget(self.details_panel)
        self.details_panel.hide()
        self.results_splitter.setStretchFactor(0, 1)
        self.results_splitter.setStretchFactor(1, 0)

        results_layout.addWidget(self.results_splitter, 1)
        self.tabs.addTab(results_tab, "Results")

        # History tab
        history_tab = QWidget()
        h = QVBoxLayout(history_tab)
        self.history_table = QTableWidget(0, 9)
        self.history_table.setHorizontalHeaderLabels([
            "ID", "Date", "Keyword", "Country", "City", "State", "Target", "Sources", "Found"
        ])
        self.history_table.horizontalHeader().setStretchLastSection(True)
        self.history_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.history_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.history_table.cellDoubleClicked.connect(self._open_history)
        h.addWidget(QLabel("Double-click a previous search to reopen its results."))
        h.addWidget(self.history_table)
        self.tabs.addTab(history_tab, "History")

    def _labeled(self, label: str, widget: QWidget) -> QWidget:
        wrap = QWidget()
        lay = QVBoxLayout(wrap)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(4)
        lay.addWidget(QLabel(label))
        lay.addWidget(widget)
        return wrap

    def _change_zoom(self, delta: float):
        self.ui_scale = max(0.8, min(1.5, round(self.ui_scale + delta, 1)))
        self.zoom_label.setText(f"{int(self.ui_scale * 100)}%")
        self._apply_style()

    def _reset_zoom(self):
        self.ui_scale = 1.0
        self.zoom_label.setText("100%")
        self._apply_style()

    def _apply_style(self):
        # Professional navy / electric-blue desktop theme with UI zoom support.
        base = max(11, int(14 * self.ui_scale))
        title_size = max(22, int(28 * self.ui_scale))
        card_value_size = max(20, int(25 * self.ui_scale))
        details_title_size = max(18, int(22 * self.ui_scale))
        style = """
            QMainWindow, QWidget {
                background: #070B14;
                color: #EAF2FF;
                font-size: {base}px;
                font-family: "Segoe UI";
            }
            #pageTitle {
                font-size: {title_size}px;
                font-weight: 800;
                color: #F7FAFF;
            }
            #subtitle { color: #7F90AB; }

            #panel, #card {
                background: #0D1422;
                border: 1px solid #1B2A42;
                border-radius: 12px;
            }
            #cardTitle {
                color: #8392AA;
                font-size: 12px;
                font-weight: 600;
            }
            #cardValue {
                font-size: {card_value_size}px;
                font-weight: 800;
                color: #2F80FF;
            }
            #accentText {
                color: #39A0FF;
                font-weight: 800;
            }

            QLabel { background: transparent; }
            QLineEdit, QComboBox {
                background: #111A2A;
                color: #F4F8FF;
                border: 1px solid #263A57;
                border-radius: 8px;
                padding: 8px 10px;
                selection-background-color: #1F6FEB;
            }
            QLineEdit:focus, QComboBox:focus {
                border: 1px solid #2F80FF;
                background: #101C30;
            }
            QComboBox::drop-down { border: none; width: 24px; }
            QComboBox QAbstractItemView {
                background: #101827;
                color: #EAF2FF;
                border: 1px solid #263A57;
                selection-background-color: #1559B7;
            }

            QPushButton {
                background: #111A2A;
                color: #EAF2FF;
                border: 1px solid #2A3E5D;
                border-radius: 8px;
                padding: 8px 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: #16243A;
                border-color: #2F80FF;
            }
            QPushButton:pressed { background: #0B65D8; }
            QPushButton:disabled {
                color: #5D6B80;
                background: #0B101A;
                border-color: #182235;
            }
            #primaryButton {
                background: #0B6BEE;
                color: white;
                border: 1px solid #2587FF;
                font-weight: 800;
                padding: 10px 22px;
            }
            #primaryButton:hover { background: #1680FF; }
            #detailsButton {
                background: #102845;
                color: #63B3FF;
                border: 1px solid #245D96;
                padding: 6px 10px;
                font-weight: 700;
            }
            #detailsButton:hover {
                background: #17406E;
                color: white;
            }

            #miniPanel {
                background: #0A111D;
                border: 1px solid #17263B;
                border-radius: 9px;
            }

            #secondaryAction {
                background: #0D1728;
                color: #B9D9FF;
                border: 1px solid #244F7B;
                padding: 8px 12px;
            }

            #secondaryAction:hover {
                background: #13243B;
                border-color: #2F80FF;
                color: white;
            }

            #compactButton {
                background: #101A2A;
                color: #9CCEFF;
                border: 1px solid #27486A;
                padding: 6px 11px;
                font-weight: 700;
            }

            #compactButton:hover {
                background: #16263C;
                color: white;
                border-color: #2F80FF;
            }

            #stopButton {
                background: #7F1D1D;
                color: white;
                border: 1px solid #EF4444;
                font-weight: 800;
            }

            #stopButton:hover {
                background: #991B1B;
            }

            #cacheBadge, #timerBadge {
                background: #0C1625;
                color: #91A8C6;
                border: 1px solid #203653;
                border-radius: 7px;
                padding: 6px 10px;
                font-weight: 650;
            }

            #timerBadge {
                color: #A9CEF7;
                padding: 5px 12px;
            }

            #mutedLabel {
                color: #8090A8;
                font-weight: 700;
            }

            #softDivider {
                color: #203653;
                background: #203653;
                max-width: 1px;
            }

            QCheckBox { spacing: 7px; color: #D9E5F7; }
            QCheckBox::indicator {
                width: 16px;
                height: 16px;
                border: 1px solid #36506F;
                border-radius: 4px;
                background: #0A111D;
            }
            QCheckBox::indicator:checked {
                background: #1478F2;
                border-color: #3B9CFF;
            }

            QSlider::groove:horizontal {
                height: 6px;
                background: #1B2739;
                border-radius: 3px;
            }
            QSlider::sub-page:horizontal {
                background: #1785FF;
                border-radius: 3px;
            }
            QSlider::handle:horizontal {
                width: 18px;
                margin: -6px 0;
                background: #45A6FF;
                border: 2px solid #A9D6FF;
                border-radius: 9px;
            }

            QTableWidget {
                background: #0B111D;
                alternate-background-color: #0E1726;
                color: #EAF2FF;
                border: 1px solid #1B2A42;
                border-radius: 9px;
                gridline-color: #16233A;
                selection-background-color: #123E72;
                selection-color: white;
            }
            QHeaderView::section {
                background: #101A2B;
                color: #A9B8CE;
                border: none;
                border-right: 1px solid #1B2A42;
                border-bottom: 1px solid #21324B;
                padding: 9px;
                font-weight: 700;
            }
            QTableCornerButton::section { background: #101A2B; border: none; }

            QProgressBar {
                background: #0B111D;
                color: #DDEBFF;
                border: 1px solid #1C2D47;
                border-radius: 8px;
                text-align: center;
                min-height: 20px;
                font-weight: 700;
            }
            QProgressBar::chunk {
                background: #137BFA;
                border-radius: 7px;
            }

            QTabWidget::pane {
                border: 1px solid #1B2A42;
                background: #090F19;
                border-radius: 9px;
            }
            QTabBar::tab {
                background: #0D1422;
                color: #8090A8;
                padding: 10px 20px;
                margin-right: 4px;
                border: 1px solid #18253A;
                border-bottom: none;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
            }
            QTabBar::tab:hover { color: #D6E6FF; background: #121E31; }
            QTabBar::tab:selected {
                background: #0D1728;
                color: #49A8FF;
                font-weight: 800;
                border-color: #244F7B;
            }

            #detailsTitle {
                font-size: {details_title_size}px;
                font-weight: 800;
                color: #F7FAFF;
            }
            #detailFieldName {
                color: #7F90AB;
                font-weight: 700;
                min-width: 105px;
            }
            #detailValueImportant {
                color: #63B3FF;
                font-weight: 650;
            }
            #detailsPanel {
                background: #0D1422;
                border: 1px solid #24405F;
                border-radius: 10px;
            }
            #closeDetailsButton {
                background: #121C2C;
                color: #C8D8EE;
                border: 1px solid #2B3F5B;
                border-radius: 7px;
                padding: 0px;
                font-weight: 900;
            }
            #closeDetailsButton:hover {
                background: #7A2632;
                color: white;
                border-color: #A83C4A;
            }
            #zoomButton {
                background: #0D1728;
                color: #63B3FF;
                border: 1px solid #244F7B;
                padding: 5px;
                font-size: {base + 2}px;
                font-weight: 800;
            }
            #zoomLabel {
                color: #9CCEFF;
                font-weight: 700;
            }

            QScrollBar:vertical {
                background: #080D16;
                width: 12px;
                margin: 0;
            }
            QScrollBar::handle:vertical {
                background: #263B58;
                min-height: 30px;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical:hover { background: #31547A; }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
            QScrollBar:horizontal {
                background: #080D16;
                height: 12px;
            }
            QScrollBar::handle:horizontal {
                background: #263B58;
                min-width: 30px;
                border-radius: 6px;
            }
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }

            QSplitter::handle { background: #111C2D; }
        """
        style = (style
                 .replace("{base}", str(base))
                 .replace("{title_size}", str(title_size))
                 .replace("{card_value_size}", str(card_value_size))
                 .replace("{details_title_size}", str(details_title_size))
                 .replace("{base + 2}", str(base + 2)))
        self.setStyleSheet(style)
        self.table.setAlternatingRowColors(True)
        self.history_table.setAlternatingRowColors(True)

    def _use_current_location(self):
        QMessageBox.information(
            self,
            "Current Location",
            "Desktop EXE location detection needs an IP/geolocation provider or Windows location permission. "
            "The GUI hook is ready; we can connect your preferred provider in the next step."
        )

    def search_or_cancel(self):
        if self.worker is not None and self.thread is not None and self.thread.isRunning():
            self.stop_search()
        else:
            self.start_search()

    def pause_or_resume_search(self):
        if self.worker is None or self.thread is None or not self.thread.isRunning():
            return

        if getattr(self, "_search_paused", False):
            self.worker.resume()
            self._search_paused = False
            self._timeout_window_started = time.monotonic()
            self.pause_btn.setText("Pause")
            self.pause_btn.setStyleSheet("")
            self.status_label.setText(
                f"Searching... {len(self.current_leads)} leads found"
            )
            self.progress.setFormat("Resumed  %p%")
        else:
            self.worker.pause()
            self._search_paused = True
            self.pause_btn.setText("Resume")
            self.pause_btn.setStyleSheet(
                "background:#8a5a00; color:white; border:1px solid #d99a24;"
            )
            self.status_label.setText(
                f"Paused — {len(self.current_leads)} leads kept"
            )
            self.progress.setFormat("Paused  %p%")

        self._save_checkpoint()

    def stop_search(self):
        if self.worker is None:
            return

        self._search_paused = False
        self.pause_btn.setText("Pause")
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setText("Stopping...")
        self.status_label.setText(
            f"Stopping... keeping {len(self.current_leads)} leads"
        )
        self.progress.setFormat("Stopping...")
        self.worker.cancel()

    def cancel_search(self):
        self.stop_search()

    def _set_search_controls_idle(self):
        self._search_paused = False

        self.search_btn.setText("Search Leads")
        self.search_btn.setEnabled(True)
        self.search_btn.setStyleSheet("")

        self.pause_btn.setText("Pause")
        self.pause_btn.setEnabled(False)
        self.pause_btn.setStyleSheet("")

        self.stop_btn.setText("Stop")
        self.stop_btn.setEnabled(False)
        self.stop_btn.setStyleSheet("")

    def start_search(self):
        country = self.country.currentText().strip()
        city = self.city.text().strip()
        if not country and not city:
            QMessageBox.warning(self, "Location required", "Enter at least a Country or City.")
            return

        keyword = self.keyword.currentText().strip()
        if not keyword:
            QMessageBox.warning(self, "Keyword required", "Enter or select a keyword/niche.")
            return

        selected_sources = [name for name, cb in self.source_checks.items() if cb.isChecked()]
        if not selected_sources:
            QMessageBox.warning(self, "Sources required", "Select at least one source.")
            return

        params = {
            "keyword": keyword,
            "country": country,
            "city": city,
            "state": self.state.text().strip(),
            "zip": self.zip_code.text().strip(),
            "target_leads": self.lead_slider.value(),
            "sources": selected_sources,
            "search_mode": self.search_mode.currentText(),
            "skip_previous": self.skip_previous.isChecked(),
            "timeout_minutes": self._selected_timeout_minutes(),
        }

        resume_leads = list(getattr(self, "_resume_base_leads", []) or [])
        resume_elapsed = int(getattr(self, "_resume_elapsed_offset", 0) or 0)

        self._search_paused = False
        self._timeout_minutes = int(params.get("timeout_minutes", 0) or 0)
        self._timeout_window_started = time.monotonic()
        self._last_checkpoint_save = -1

        self.search_btn.setEnabled(False)
        self.search_btn.setText("Searching...")

        self.pause_btn.setText("Pause")
        self.pause_btn.setEnabled(True)
        self.pause_btn.setStyleSheet("")

        self.stop_btn.setText("Stop")
        self.stop_btn.setEnabled(True)
        self.stop_btn.setStyleSheet("")

        self.progress.setValue(0)
        self.status_label.setText("Searching... 0 leads found")
        self.table.setRowCount(0)
        self._close_details_panel()
        self.current_leads = []
        self._live_lead_keys = set()
        self.total_card.value.setText("0")

        if resume_leads:
            self.current_leads = resume_leads
            self._live_lead_keys = {
                key
                for key in (self._live_lead_key(lead) for lead in resume_leads)
                if key
            }
            self._populate_results(self.current_leads)
            self.total_card.value.setText(str(len(self.current_leads)))
            self.status_label.setText(
                f"Resumed — {len(self.current_leads)} saved leads loaded"
            )
        for c in self.source_cards.values():
            c.value.setText("0")

        self._search_started_at = time.monotonic() - resume_elapsed
        self._last_elapsed_seconds = resume_elapsed
        self._update_elapsed_display()
        self.search_timer.start()
        self._refresh_cache_info()

        self.thread = QThread()
        self.worker = SearchWorker(params)
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self._on_progress)
        self.worker.source_progress.connect(self._on_source_progress)
        self.worker.lead_found.connect(self._on_lead_found)
        self._active_search_params = params
        self.worker.finished.connect(self._search_finished)
        self.worker.cancelled.connect(self._search_cancelled)
        self.worker.error.connect(self._search_error)
        self.worker.finished.connect(self.thread.quit)
        self.worker.cancelled.connect(self.thread.quit)
        self.worker.error.connect(self.thread.quit)
        self.thread.finished.connect(self._cleanup_thread)
        self.thread.start()

    def _on_progress(self, value: int, text: str):
        self.progress.setValue(value)
        self.progress.setFormat(text + "  %p%")

    def _on_source_progress(self, source: str, count: int):
        if source in self.source_cards:
            self.source_cards[source].value.setText(str(count))

    @Slot(dict)
    def _on_lead_found(self, raw_lead: dict):
        try:
            lead = Lead(**raw_lead)
        except Exception as exc:
            print(f"Could not display streamed lead: {exc}")
            return

        key = self._live_lead_key(lead)

        if key and key in getattr(self, "_live_lead_keys", set()):
            return

        if key:
            self._live_lead_keys.add(key)

        self.current_leads.append(lead)
        self._append_live_lead_row(lead)

        count = len(self.current_leads)
        self.total_card.value.setText(str(count))
        self._update_elapsed_display()

        if getattr(self, "_search_paused", False):
            self.status_label.setText(f"Paused — {count} leads kept")
        else:
            self.status_label.setText(f"Searching... {count} leads found")

    def _live_lead_key(self, lead: Lead) -> str:
        import re as _re
        from urllib.parse import urlparse as _urlparse

        maps_url = str(getattr(lead, "google_maps_url", "") or "").strip()
        if maps_url:
            return "maps:" + maps_url

        phone = _re.sub(r"\D+", "", str(lead.phone or ""))
        if phone:
            return "phone:" + phone

        website = str(lead.website or "").strip()
        if website:
            try:
                url = website if "://" in website else "https://" + website
                host = _urlparse(url).netloc.lower().replace("www.", "")
                if host:
                    return "site:" + host
            except Exception:
                pass

        name = _re.sub(r"[^a-z0-9]+", "", str(lead.name or "").lower())
        address = _re.sub(r"[^a-z0-9]+", "", str(lead.address or "").lower())
        city = _re.sub(r"[^a-z0-9]+", "", str(lead.city or "").lower())

        if name:
            return f"name:{name}|{address}|{city}"

        return ""

    def _append_live_lead_row(self, lead: Lead):
        row = self.table.rowCount()
        self.table.insertRow(row)

        location_parts = [x for x in [lead.city, lead.state] if x]
        location = ", ".join(location_parts) or lead.country or ""

        values = [
            str(row + 1),
            lead.name,
            location,
            lead.phone,
            lead.sources or lead.source,
        ]

        for column, value in enumerate(values):
            item = QTableWidgetItem(str(value or ""))
            item.setToolTip(str(value or ""))
            if column == 0:
                item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row, column, item)

        details_btn = QPushButton("View Details")
        details_btn.setObjectName("detailsButton")
        details_btn.clicked.connect(
            lambda _checked=False, selected_row=row: self._open_lead_details(selected_row)
        )
        self.table.setCellWidget(row, 5, details_btn)
        self.table.setRowHeight(row, max(46, int(48 * self.ui_scale)))

        if row == 0:
            self.table.selectRow(0)
            self._show_selected_lead()


    def _remove_clean_exit_marker(self):
        try:
            if os.path.exists(CLEAN_EXIT_MARKER_PATH):
                os.remove(CLEAN_EXIT_MARKER_PATH)
        except Exception:
            pass

    def _write_clean_exit_marker(self):
        try:
            with open(CLEAN_EXIT_MARKER_PATH, "w", encoding="utf-8") as fh:
                fh.write(datetime.now().isoformat(timespec="seconds"))
        except Exception:
            pass

    def _clear_seen_cache_for_clean_exit(self):
        """
        Clear persistent seen-cache only on a deliberate user exit when there
        is no unfinished search checkpoint that still needs duplicate
        protection for resume.
        """
        if self._saved_incomplete_search_on_exit:
            return

        try:
            cache = SeenLeadCache(max_rows=20000)

            # Prefer a public clear-all method if available.
            if hasattr(cache, "clear_all"):
                cache.clear_all()
                return

            # Fallback: remove the SQLite cache file directly if the cache
            # implementation exposes no public method. DB path variants are
            # checked conservatively.
            candidates = []

            for attr in ("db_path", "path", "database_path"):
                value = getattr(cache, attr, None)
                if value:
                    candidates.append(str(value))

            candidates.extend([
                os.path.join(
                    os.path.dirname(DB_PATH),
                    "lead_seen_cache.db",
                ),
                os.path.join(
                    os.environ.get("LOCALAPPDATA", os.path.dirname(DB_PATH)),
                    "USA Lead Finder",
                    "lead_seen_cache.db",
                ),
            ])

            for path in dict.fromkeys(candidates):
                try:
                    if path and os.path.exists(path):
                        os.remove(path)
                except Exception:
                    pass

        except Exception as exc:
            print(f"Clean-exit cache clear failed: {exc}")

    def _selected_timeout_minutes(self):
        value = self.timeout_combo.currentText().strip()
        if value == "No Limit":
            return 0
        match = re.search(r"(\d+)", value)
        return int(match.group(1)) if match else 0

    def _save_checkpoint(self, force=False):
        running = (
            self.thread is not None
            and self.thread.isRunning()
            and self.worker is not None
        )

        if not force and not running:
            return

        try:
            params = dict(
                getattr(
                    self,
                    "_active_search_params",
                    {
                        "keyword": self.keyword.currentText().strip(),
                        "country": self.country.currentText().strip(),
                        "city": self.city.text().strip(),
                        "state": self.state.text().strip(),
                        "zip": self.zip_code.text().strip(),
                        "target_leads": self.lead_slider.value(),
                        "sources": [
                            name
                            for name, cb in self.source_checks.items()
                            if cb.isChecked()
                        ],
                        "search_mode": self.search_mode.currentText(),
                        "skip_previous": self.skip_previous.isChecked(),
                        "timeout_minutes": self._selected_timeout_minutes(),
                    },
                )
            )

            elapsed = self._last_elapsed_seconds
            if self._search_started_at is not None:
                elapsed = max(
                    0,
                    int(time.monotonic() - self._search_started_at),
                )

            payload = {
                "version": 1,
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "params": params,
                "elapsed_seconds": int(elapsed),
                "paused": bool(getattr(self, "_search_paused", False)),
                "leads": [asdict(lead) for lead in self.current_leads],
            }

            temp_path = CHECKPOINT_PATH + ".tmp"

            with open(temp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)

            os.replace(temp_path, CHECKPOINT_PATH)

        except Exception as exc:
            print(f"Checkpoint save failed: {exc}")

    def _clear_checkpoint(self):
        try:
            if os.path.exists(CHECKPOINT_PATH):
                os.remove(CHECKPOINT_PATH)
        except Exception:
            pass

    def _load_checkpoint(self):
        if not os.path.exists(CHECKPOINT_PATH):
            return None

        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)

            if not isinstance(data, dict):
                return None

            if not isinstance(data.get("params"), dict):
                return None

            return data

        except Exception:
            return None

    def _offer_resume_checkpoint(self):
        data = self._load_checkpoint()
        if not data:
            return

        params = data.get("params", {})
        count = len(data.get("leads", []))
        keyword = params.get("keyword", "Previous search")
        place = params.get("city") or params.get("country") or ""
        elapsed = self._format_elapsed(
            int(data.get("elapsed_seconds", 0) or 0)
        )

        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle("Resume Interrupted Search")
        box.setText("An interrupted search was found.")
        box.setInformativeText(
            f"{keyword} / {place}\n"
            f"{count} leads saved • {elapsed} elapsed\n\n"
            "Resume this search?"
        )

        resume_btn = box.addButton(
            "Resume Search",
            QMessageBox.AcceptRole,
        )
        discard_btn = box.addButton(
            "Discard",
            QMessageBox.DestructiveRole,
        )
        box.setDefaultButton(resume_btn)
        box.exec()

        if box.clickedButton() == resume_btn:
            self._resume_checkpoint(data)
        elif box.clickedButton() == discard_btn:
            self._clear_checkpoint()

    def _resume_checkpoint(self, data):
        params = dict(data.get("params", {}))

        self.keyword.setCurrentText(str(params.get("keyword", "")))
        self.country.setCurrentText(str(params.get("country", "USA")))
        self.city.setText(str(params.get("city", "")))
        self.state.setText(str(params.get("state", "")))
        self.zip_code.setText(str(params.get("zip", "")))

        target = int(params.get("target_leads", 100) or 100)
        target = max(
            self.lead_slider.minimum(),
            min(self.lead_slider.maximum(), target),
        )
        self.lead_slider.setValue(target)

        self.search_mode.setCurrentText(
            str(params.get("search_mode", "Balanced"))
        )
        self.skip_previous.setChecked(
            bool(params.get("skip_previous", True))
        )

        timeout_minutes = int(params.get("timeout_minutes", 0) or 0)
        timeout_text = (
            "No Limit"
            if timeout_minutes <= 0
            else f"{timeout_minutes} min"
        )
        if self.timeout_combo.findText(timeout_text) >= 0:
            self.timeout_combo.setCurrentText(timeout_text)

        selected = set(params.get("sources", []))
        for name, cb in self.source_checks.items():
            cb.setChecked(name in selected)

        recovered = []
        for raw in data.get("leads", []):
            try:
                recovered.append(Lead(**raw))
            except Exception:
                pass

        self._resume_base_leads = recovered
        self._resume_elapsed_offset = int(
            data.get("elapsed_seconds", 0) or 0
        )

        # Add recovered leads to persistent seen-cache so the restarted
        # workers continue beyond those businesses.
        try:
            cache = SeenLeadCache(max_rows=20000)
            scope = SeenLeadCache.scope_key(
                params.get("keyword", ""),
                params.get("country", ""),
                params.get("city", ""),
                params.get("state", ""),
                params.get("zip", ""),
            )

            rows = []
            for lead in recovered:
                key = self._live_lead_key(lead)
                if key:
                    rows.append((asdict(lead), key))

            cache.add_many(scope, rows)

        except Exception as exc:
            print(f"Resume cache update failed: {exc}")

        self.start_search()

    def closeEvent(self, event):
        running = (
            self.thread is not None
            and self.thread.isRunning()
            and self.worker is not None
        )

        # No active search = deliberate clean exit.
        # Clear persistent seen-cache so next app launch starts fresh.
        if not running:
            self._intentional_exit = True
            self._saved_incomplete_search_on_exit = False

            self._clear_seen_cache_for_clean_exit()
            self._write_clean_exit_marker()

            event.accept()
            return

        # Active/incomplete search:
        # keep checkpoint + relevant seen-cache so resume does not rediscover
        # already-collected leads.
        answer = QMessageBox.question(
            self,
            "Search Still Running",
            (
                "A search is still active.\n\n"
                "Yes = save progress and exit.\n"
                "No = continue searching."
            ),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if answer != QMessageBox.Yes:
            event.ignore()
            return

        self._intentional_exit = True
        self._saved_incomplete_search_on_exit = True

        # Save unfinished work and DO NOT clear cache.
        self._save_checkpoint(force=True)
        self._write_clean_exit_marker()

        self._exit_after_stop = True
        event.ignore()
        self.stop_search()

    def _format_elapsed(self, seconds):
        seconds = max(0, int(seconds))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:02d}:{minutes:02d}:{sec:02d}"
        return f"{minutes:02d}:{sec:02d}"

    def _update_elapsed_display(self):
        if self._search_started_at is not None:
            elapsed = max(0, int(time.monotonic() - self._search_started_at))
            self._last_elapsed_seconds = elapsed
        else:
            elapsed = self._last_elapsed_seconds

        count = len(self.current_leads)
        per_lead = (elapsed / count) if count else 0
        per_text = f"{per_lead:.1f} sec/lead" if count else "-- sec/lead"
        self.time_label.setText(
            f"{count} leads  •  {self._format_elapsed(elapsed)}  •  {per_text}"
        )

        running = (
            self.thread is not None
            and self.thread.isRunning()
            and self.worker is not None
        )

        if running and elapsed // 5 != self._last_checkpoint_save:
            self._last_checkpoint_save = elapsed // 5
            self._save_checkpoint()

        if (
            running
            and not getattr(self, "_search_paused", False)
            and self._timeout_minutes > 0
            and self._timeout_window_started is not None
            and (
                time.monotonic() - self._timeout_window_started
                >= self._timeout_minutes * 60
            )
        ):
            self.pause_or_resume_search()
            self.status_label.setText(
                f"Paused after {self._timeout_minutes} minute timeout — "
                f"{len(self.current_leads)} leads saved"
            )
            self.progress.setFormat("Timeout paused — click Resume")
            self._save_checkpoint()

    def _stop_elapsed_timer(self):
        self.search_timer.stop()
        if self._search_started_at is not None:
            self._last_elapsed_seconds = max(
                0,
                int(time.monotonic() - self._search_started_at),
            )
        self._search_started_at = None
        self._update_elapsed_display()

    def _cache_scope_key(self):
        return SeenLeadCache.scope_key(
            self.keyword.currentText().strip(),
            self.country.currentText().strip(),
            self.city.text().strip(),
            self.state.text().strip(),
            self.zip_code.text().strip(),
        )

    def _refresh_cache_info(self):
        try:
            cache = SeenLeadCache(max_rows=20000)
            count = cache.count_scope(self._cache_scope_key())
            self.cache_info.setText(f"Cache: {count} previous leads")
        except Exception:
            self.cache_info.setText("Cache: unavailable")

    def _clear_current_search_cache(self):
        keyword = self.keyword.currentText().strip()
        city = self.city.text().strip()
        place = city or self.country.currentText().strip()

        if not keyword:
            QMessageBox.information(self, "Cache", "Choose a keyword first.")
            return

        answer = QMessageBox.question(
            self,
            "Clear Search Cache",
            f"Clear previously seen leads for {keyword} / {place}?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if answer != QMessageBox.Yes:
            return

        cache = SeenLeadCache(max_rows=20000)
        cache.clear_scope(self._cache_scope_key())
        self._refresh_cache_info()
        QMessageBox.information(
            self,
            "Cache",
            "Cache cleared for this search only.",
        )

    @Slot(list, dict)
    def _search_finished(self, raw_leads: list, source_counts: dict):
        self._stop_elapsed_timer()
        params = getattr(self, "_active_search_params", {})
        new_leads = [
            Lead(**x) if isinstance(x, dict) else x
            for x in raw_leads
        ]

        base_leads = list(getattr(self, "_resume_base_leads", []) or [])
        combined = []
        seen = set()

        for lead in base_leads + new_leads:
            key = self._live_lead_key(lead)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            combined.append(lead)

        requested_target = int(
            getattr(self, "_active_search_params", {}).get(
                "target_leads",
                len(combined),
            )
            or len(combined)
        )

        self.current_leads = combined[:requested_target]
        self._resume_base_leads = []
        self._resume_elapsed_offset = 0
        self._populate_results(self.current_leads)
        self.total_card.value.setText(str(len(self.current_leads)))

        # _stop_elapsed_timer() ran before the final dedup/trim, so refresh the
        # badge now with the FINAL lead count instead of the live candidate count.
        self._update_elapsed_display()
        for source, count in source_counts.items():
            if source in self.source_cards:
                self.source_cards[source].value.setText(str(count))
        if len(self.current_leads) < requested_target:
            self.status_label.setText(
                f"Fresh pool exhausted — {len(self.current_leads)} new unique leads "
                f"found in {self._format_elapsed(self._last_elapsed_seconds)} "
                f"(target {requested_target})"
            )
            self.progress.setValue(100)
            self.progress.setFormat("Fresh pool exhausted  100%")
        else:
            self.status_label.setText(
                f"Completed — {len(self.current_leads)} leads in "
                f"{self._format_elapsed(self._last_elapsed_seconds)}"
            )
            self.progress.setValue(100)
            self.progress.setFormat("Done  100%")
        self._set_search_controls_idle()
        self.db.save_search(params, self.current_leads)
        self._load_history()
        self._refresh_cache_info()
        self._clear_checkpoint()

    @Slot(list, dict)
    def _search_cancelled(self, raw_leads: list, source_counts: dict):
        self._stop_elapsed_timer()
        params = getattr(self, "_active_search_params", {})
        new_leads = [
            Lead(**x) if isinstance(x, dict) else x
            for x in raw_leads
        ]

        base_leads = list(getattr(self, "_resume_base_leads", []) or [])
        combined = []
        seen = set()

        for lead in base_leads + new_leads:
            key = self._live_lead_key(lead)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            combined.append(lead)

        requested_target = int(
            getattr(self, "_active_search_params", {}).get(
                "target_leads",
                len(combined),
            )
            or len(combined)
        )

        self.current_leads = combined[:requested_target]
        self._resume_base_leads = []
        self._resume_elapsed_offset = 0
        self._populate_results(self.current_leads)
        self.total_card.value.setText(str(len(self.current_leads)))

        # _stop_elapsed_timer() ran before the final dedup/trim, so refresh the
        # badge now with the FINAL lead count instead of the live candidate count.
        self._update_elapsed_display()
        for source, count in source_counts.items():
            if source in self.source_cards:
                self.source_cards[source].value.setText(str(count))
        self.status_label.setText(
            f"Stopped — kept {len(self.current_leads)} leads in "
            f"{self._format_elapsed(self._last_elapsed_seconds)}"
        )
        self.progress.setFormat("Stopped")
        self._set_search_controls_idle()
        if self.current_leads:
            self.db.save_search(params, self.current_leads)
            self._load_history()
        self._refresh_cache_info()

        if self._exit_after_stop:
            self._save_checkpoint(force=True)
        else:
            self._clear_checkpoint()

    def _search_error(self, text: str):
        self._stop_elapsed_timer()
        self._set_search_controls_idle()
        self.status_label.setText("Search failed")
        self.progress.setFormat("Failed")
        self._save_checkpoint(force=True)
        QMessageBox.critical(self, "Search error", text)

    def _cleanup_thread(self):
        if self.worker:
            self.worker.deleteLater()
        if self.thread:
            self.thread.deleteLater()
        self.worker = None
        self.thread = None

        if self._exit_after_stop:
            QTimer.singleShot(0, self.close)

    def _populate_results(self, leads: List[Lead]):
        self.table.setRowCount(len(leads))
        for r, lead in enumerate(leads):
            location_parts = [x for x in [lead.city, lead.state] if x]
            location = ", ".join(location_parts) or lead.country or ""
            values = [str(r + 1), lead.name, location, lead.phone, lead.sources]

            for c, value in enumerate(values):
                item = QTableWidgetItem(str(value or ""))
                item.setToolTip(str(value or ""))
                if c == 0:
                    item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(r, c, item)

            details_btn = QPushButton("View Details")
            details_btn.setObjectName("detailsButton")
            details_btn.clicked.connect(lambda _checked=False, row=r: self._open_lead_details(row))
            self.table.setCellWidget(r, 5, details_btn)
            self.table.setRowHeight(r, max(46, int(48 * self.ui_scale)))

        if leads:
            self.table.selectRow(0)
            self._show_selected_lead()
        else:
            self._clear_lead_details()
            self._close_details_panel()

    def _open_lead_details(self, row: int):
        if row < 0 or row >= len(self.current_leads):
            return
        self.table.selectRow(row)
        self._show_selected_lead()
        self.details_panel.show()
        total = max(1000, self.results_splitter.width())
        panel_width = min(520, max(410, int(total * 0.34)))
        self.results_splitter.setSizes([max(500, total - panel_width), panel_width])

    def _close_details_panel(self):
        if hasattr(self, "details_panel"):
            self.details_panel.hide()

    def _clear_lead_details(self):
        for lbl in getattr(self, "detail_labels", {}).values():
            lbl.setText("-")

    def _link_html(self, url: str, label: str = "Open Link") -> str:
        if not url:
            return "-"
        safe_url = str(url).replace('"', '%22')
        safe_label = str(label).replace("<", "&lt;").replace(">", "&gt;")
        return f'<a style="color:#63B3FF; text-decoration:none;" href="{safe_url}">{safe_label}</a>'

    def _source_links_html(self, lead: Lead) -> str:
        links = []
        seen = set()

        raw = getattr(lead, "source_links", "") or ""

        if isinstance(raw, (list, tuple, set)):
            raw_items = list(raw)
        else:
            raw_items = str(raw).splitlines()

        for item in raw_items:
            item = str(item or "").strip()

            if not item:
                continue

            if "|" in item:
                label, url = item.split("|", 1)
                label = label.strip() or "Source"
                url = url.strip()
            else:
                label = "Source"
                url = item

            if url and url not in seen:
                seen.add(url)
                links.append(
                    self._link_html(url, label)
                )

        if lead.linkedin and lead.linkedin not in seen:
            seen.add(lead.linkedin)
            links.append(
                self._link_html(
                    lead.linkedin,
                    "LinkedIn"
                )
            )

        if lead.website and lead.website not in seen:
            seen.add(lead.website)
            links.append(
                self._link_html(
                    lead.website,
                    "Website"
                )
            )

        return "<br><br>".join(links) if links else "-"

    def _show_selected_lead(self):
        row = self.table.currentRow()
        if row < 0 or row >= len(self.current_leads):
            self._clear_lead_details()
            return

        lead = self.current_leads[row]
        mapping = {
            "Name": lead.name or "-",
            "Phone": lead.phone or "-",
            "Website": self._link_html(lead.website, lead.website) if lead.website else "-",
            "Email": lead.email or "-",
            "Address": lead.address or "-",
            "Rating": lead.rating or "-",
            "Reviews": lead.reviews or "-",
            "LinkedIn": self._link_html(lead.linkedin, lead.linkedin) if lead.linkedin else "-",
            "Notes": lead.notes or "-",
            "Sources": lead.sources or lead.source or "-",
            "Source Links": self._source_links_html(lead),
        }

        for key, value in mapping.items():
            label = self.detail_labels.get(key)
            if label is not None:
                label.setText(str(value))

    def _load_history(self):
        rows = self.db.history()
        self.history_rows = rows
        self.history_table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            display = row[:9]
            for c, value in enumerate(display):
                if c == 7:
                    try:
                        value = ", ".join(json.loads(value))
                    except Exception:
                        pass
                self.history_table.setItem(r, c, QTableWidgetItem(str(value or "")))

    def _open_history(self, row: int, col: int):
        data = self.history_rows[row]
        results_json = data[9]
        try:
            leads = [Lead(**x) for x in json.loads(results_json)]
        except Exception as exc:
            QMessageBox.warning(self, "History", f"Could not load history: {exc}")
            return
        self.current_leads = leads
        self._populate_results(leads)
        self.total_card.value.setText(str(len(leads)))
        counts = {s: 0 for s in SOURCES}
        for lead in leads:
            for src in [x.strip() for x in lead.sources.split(",") if x.strip()]:
                if src in counts:
                    counts[src] += 1
        for s, n in counts.items():
            self.source_cards[s].value.setText(str(n))
        self.tabs.setCurrentIndex(0)
        self.status_label.setText(f"Loaded history search #{data[0]}")

    def export_csv(self):
        if not self.current_leads:
            QMessageBox.information(self, "Export", "There are no leads to export.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "leads.csv", "CSV Files (*.csv)")
        if not path:
            return
        import csv
        fields = list(asdict(self.current_leads[0]).keys())
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for lead in self.current_leads:
                writer.writerow(asdict(lead))
        QMessageBox.information(self, "Export", f"Exported {len(self.current_leads)} leads.")


    def force_shutdown(self):
        """
        Best-effort cleanup before the Python process exits.
        This prevents the console from remaining occupied by background
        scraper/enrichment threads after the window has closed.
        """
        try:
            self.search_timer.stop()
        except Exception:
            pass

        try:
            if self.worker is not None:
                self.worker.cancel()
        except Exception:
            pass

        try:
            # SourceManager cancellation ultimately kills active Google Maps /
            # Chromium child processes too.
            if self.worker is not None and getattr(self.worker, "manager", None):
                self.worker.manager.cancel()
        except Exception:
            pass

        try:
            if self.thread is not None and self.thread.isRunning():
                self.thread.quit()
                self.thread.wait(1500)
        except Exception:
            pass


def main():
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)

    window = MainWindow()
    window.show()

    exit_code = app.exec()

    # Qt can close while Python ThreadPoolExecutor / network workers are still
    # alive. Clean them up, then terminate this Python process so the terminal
    # prompt is returned immediately.
    try:
        window.force_shutdown()
    except Exception:
        pass

    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass

    os._exit(int(exit_code))


if __name__ == "__main__":
    main()
