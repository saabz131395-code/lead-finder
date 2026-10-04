import csv
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from pathlib import Path


# Path to the external Google Maps scraper project.
# Set the GMAPS_SCRAPER_PATH environment variable to point at your copy.
REPO_PATH = Path(
    os.environ.get(
        "GMAPS_SCRAPER_PATH",
        str(Path.home() / "Desktop" / "Lead-Scraper-Google-Maps"),
    )
)
VENV_PYTHON = (
    REPO_PATH / ".venv" / "Scripts" / "python.exe"
    if os.name == "nt"
    else REPO_PATH / ".venv" / "bin" / "python"
)


# ---------------------------------------------------------
# Active Google Maps process
# ---------------------------------------------------------

_process_lock = threading.Lock()
_active_process = None


def _set_active_process(process):
    global _active_process

    with _process_lock:
        _active_process = process


def _get_active_process():
    with _process_lock:
        return _active_process


def _clear_active_process(process=None):
    global _active_process

    with _process_lock:
        if process is None or _active_process is process:
            _active_process = None


# ---------------------------------------------------------
# Process-tree helpers
# ---------------------------------------------------------

def _get_process_tree(pid):
    """
    Return psutil process + children.

    psutil will be installed in the next step.
    """

    try:
        import psutil
    except ImportError:
        return []

    try:
        parent = psutil.Process(pid)

        children = parent.children(
            recursive=True
        )

        return children + [parent]

    except Exception:
        return []


# ---------------------------------------------------------
# PAUSE
# ---------------------------------------------------------

def pause_google_maps():
    """
    Pause Python scraper + Chromium child processes.
    """

    process = _get_active_process()

    if process is None:
        return False

    if process.poll() is not None:
        return False

    tree = _get_process_tree(
        process.pid
    )

    if not tree:
        return False

    # Children first, then parent
    for proc in tree:
        try:
            proc.suspend()
        except Exception:
            pass

    return True


# ---------------------------------------------------------
# RESUME
# ---------------------------------------------------------

def resume_google_maps():
    """
    Resume paused Python scraper + Chromium processes.
    """

    process = _get_active_process()

    if process is None:
        return False

    if process.poll() is not None:
        return False

    tree = _get_process_tree(
        process.pid
    )

    if not tree:
        return False

    # Parent first is normally safest for resume
    for proc in reversed(tree):
        try:
            proc.resume()
        except Exception:
            pass

    return True


# ---------------------------------------------------------
# STOP
# ---------------------------------------------------------

def cancel_google_maps():
    """
    Completely stop the current Google Maps scrape.

    Already streamed leads remain inside the GUI.
    """

    process = _get_active_process()

    if process is None:
        return False

    try:
        if process.poll() is not None:
            return True

        # If processes were suspended, resume before termination.
        try:
            resume_google_maps()
        except Exception:
            pass

        if os.name == "nt":

            subprocess.run(
                [
                    "taskkill",
                    "/PID",
                    str(process.pid),
                    "/T",
                    "/F",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )

        else:

            process.terminate()

            try:
                process.wait(
                    timeout=3
                )

            except subprocess.TimeoutExpired:
                process.kill()

    except Exception:

        try:
            process.kill()
        except Exception:
            pass

    finally:
        _clear_active_process(
            process
        )

    return True


stop_google_maps = cancel_google_maps


# ---------------------------------------------------------
# STDOUT READER THREAD
# ---------------------------------------------------------

def _stdout_reader(
    process,
    output_queue,
):
    """
    Read subprocess stdout without blocking the main streaming loop.
    """

    try:

        if process.stdout is None:
            return

        for line in iter(
            process.stdout.readline,
            "",
        ):

            if not line:
                break

            output_queue.put(
                line.rstrip()
            )

    except Exception:
        pass

    finally:
        output_queue.put(
            None
        )


# ---------------------------------------------------------
# MAIN SEARCH
# ---------------------------------------------------------

def search_google_maps(
    keyword,
    city="",
    state="",
    country="USA",
    limit=20,
    progress_cb=None,
    lead_cb=None,
    cancel_event=None,
):
    """
    Run the Google Maps scraper.

    progress_cb(percent, message)
        Sends live progress messages.

    lead_cb(lead_dict)
        Called whenever a new lead becomes available.

    cancel_event
        Callable or threading.Event checked continuously.

    Returns:
        Full list of leads collected before completion/stop.
    """

    keyword = str(
        keyword or ""
    ).strip()

    city = str(
        city or ""
    ).strip()

    state = str(
        state or ""
    ).strip()

    country = str(
        country or "USA"
    ).strip()

    if not keyword:
        raise ValueError(
            "Keyword is required."
        )

    location_parts = [
        x
        for x in [
            city,
            state,
            country,
        ]
        if x
    ]

    location = " ".join(
        location_parts
    ).strip()

    if not location:
        raise ValueError(
            "City or location is required."
        )

    if not REPO_PATH.exists():
        raise FileNotFoundError(
            f"Google Maps repository not found:\n{REPO_PATH}"
        )

    if not VENV_PYTHON.exists():
        raise FileNotFoundError(
            f"Google Maps Python environment not found:\n{VENV_PYTHON}"
        )

    limit = max(
        1,
        int(limit),
    )

    # ---------------------------------------------
    # callbacks
    # ---------------------------------------------

    def report(
        percent,
        message,
    ):

        if progress_cb is None:
            return

        try:
            progress_cb(
                int(percent),
                str(message),
            )
        except Exception:
            pass

    def emit_lead(lead):

        if lead_cb is None:
            return

        try:
            lead_cb(
                dict(lead)
            )
        except Exception:
            pass

    def is_cancelled():

        if cancel_event is None:
            return False

        try:

            if callable(
                cancel_event
            ):
                return bool(
                    cancel_event()
                )

            if hasattr(
                cancel_event,
                "is_set",
            ):
                return bool(
                    cancel_event.is_set()
                )

        except Exception:
            pass

        return False

    # ---------------------------------------------
    # temporary independent search folder
    # ---------------------------------------------

    report(
        2,
        "Starting Google Maps...",
    )

    collected_leads = []

    emitted_keys = set()

    with tempfile.TemporaryDirectory(
        prefix="lead_maps_"
    ) as temp_dir:

        output_dir = Path(
            temp_dir
        )

        raw_csv = (
            output_dir
            / "leads_raw.csv"
        )

        command = [
            str(VENV_PYTHON),

            "-u",

            "main.py",

            "--city",
            location,

            "--categories",
            keyword,

            "--profile",
            "all",

            "--max-results",
            str(limit),

            "--headless",

            "--no-tui",

            "--yes",

            "--no-resume",

            "--output",
            str(output_dir),
        ]

        startupinfo = None
        creationflags = 0

        if os.name == "nt":

            startupinfo = (
                subprocess.STARTUPINFO()
            )

            startupinfo.dwFlags |= (
                subprocess.STARTF_USESHOWWINDOW
            )

            creationflags = (
                subprocess.CREATE_NO_WINDOW
                |
                subprocess.CREATE_NEW_PROCESS_GROUP
            )

        process = subprocess.Popen(
            command,

            cwd=str(
                REPO_PATH
            ),

            stdout=subprocess.PIPE,

            stderr=subprocess.STDOUT,

            text=True,

            encoding="utf-8",

            errors="replace",

            bufsize=1,

            startupinfo=startupinfo,

            creationflags=creationflags,
        )

        _set_active_process(
            process
        )

        stdout_queue = (
            queue.Queue()
        )

        reader_thread = threading.Thread(
            target=_stdout_reader,
            args=(
                process,
                stdout_queue,
            ),
            daemon=True,
        )

        reader_thread.start()

        output_lines = []

        last_csv_check = 0.0

        try:

            while True:

                # ---------------------------------
                # STOP requested
                # ---------------------------------

                if is_cancelled():

                    report(
                        100,
                        "Stopping search...",
                    )

                    # Read anything already written
                    # before killing the process.
                    _stream_new_csv_leads(
                        raw_csv=raw_csv,
                        keyword=keyword,
                        fallback_city=city,
                        fallback_state=state,
                        country=country,
                        emitted_keys=emitted_keys,
                        collected_leads=collected_leads,
                        lead_cb=emit_lead,
                    )

                    cancel_google_maps()

                    report(
                        100,
                        f"Stopped - kept {len(collected_leads)} leads",
                    )

                    return collected_leads[
                        :limit
                    ]

                # ---------------------------------
                # Consume stdout messages
                # ---------------------------------

                while True:

                    try:
                        line = (
                            stdout_queue
                            .get_nowait()
                        )

                    except queue.Empty:
                        break

                    if line is None:
                        break

                    output_lines.append(
                        line
                    )

                    percent, message = (
                        _parse_progress_line(
                            line
                        )
                    )

                    if message:
                        report(
                            percent,
                            message,
                        )

                # ---------------------------------
                # STREAM NEW CSV ROWS
                # ---------------------------------

                now = time.time()

                if (
                    now
                    - last_csv_check
                    >= 0.35
                ):

                    last_csv_check = now

                    _stream_new_csv_leads(
                        raw_csv=raw_csv,
                        keyword=keyword,
                        fallback_city=city,
                        fallback_state=state,
                        country=country,
                        emitted_keys=emitted_keys,
                        collected_leads=collected_leads,
                        lead_cb=emit_lead,
                    )

                    if collected_leads:

                        report(
                            min(
                                90,
                                40
                                + len(
                                    collected_leads
                                )
                                * 4,
                            ),
                            (
                                f"Found "
                                f"{len(collected_leads)} "
                                f"lead"
                                f"{'s' if len(collected_leads) != 1 else ''}..."
                            ),
                        )

                # ---------------------------------
                # Process completed
                # ---------------------------------

                if process.poll() is not None:
                    break

                time.sleep(
                    0.10
                )

            return_code = (
                process.returncode
            )

            # One final read after the process exits.
            _stream_new_csv_leads(
                raw_csv=raw_csv,
                keyword=keyword,
                fallback_city=city,
                fallback_state=state,
                country=country,
                emitted_keys=emitted_keys,
                collected_leads=collected_leads,
                lead_cb=emit_lead,
            )

        finally:

            _clear_active_process(
                process
            )

        if is_cancelled():

            return collected_leads[
                :limit
            ]

        if return_code != 0:

            error_text = "\n".join(
                output_lines[-50:]
            )

            raise RuntimeError(
                "Google Maps scraper failed.\n\n"
                + error_text
            )

        report(
            100,
            (
                f"Google Maps found "
                f"{len(collected_leads)} leads"
            ),
        )

        return collected_leads[
            :limit
        ]


# ---------------------------------------------------------
# LIVE CSV STREAMER
# ---------------------------------------------------------

def _stream_new_csv_leads(
    raw_csv,
    keyword,
    fallback_city,
    fallback_state,
    country,
    emitted_keys,
    collected_leads,
    lead_cb,
):
    """
    Look for newly written CSV rows and emit only rows
    that have not already been sent to the GUI.
    """

    if not raw_csv.exists():
        return

    try:

        with open(
            raw_csv,
            "r",
            encoding="utf-8-sig",
            errors="replace",
            newline="",
        ) as file:

            reader = (
                csv.DictReader(
                    file
                )
            )

            for row in reader:

                lead = _row_to_lead(
                    row=row,
                    keyword=keyword,
                    fallback_city=fallback_city,
                    fallback_state=fallback_state,
                    country=country,
                )

                key = _lead_key(
                    lead
                )

                if not key:
                    continue

                if key in emitted_keys:
                    continue

                emitted_keys.add(
                    key
                )

                collected_leads.append(
                    lead
                )

                try:
                    lead_cb(
                        lead
                    )
                except Exception:
                    pass

    except (
        PermissionError,
        OSError,
        csv.Error,
    ):
        # File may be in the middle of being written.
        # Simply retry on the next polling cycle.
        return


# ---------------------------------------------------------
# ROW -> GUI LEAD
# ---------------------------------------------------------

def _row_to_lead(
    row,
    keyword,
    fallback_city,
    fallback_state,
    country,
):

    name = clean(
        row.get("Name")
    )

    phone = clean(
        row.get("Phone")
    )

    website = clean(
        row.get(
            "Website URL"
        )
    )

    email = clean(
        row.get(
            "Business Email"
        )
    )

    address = clean(
        row.get("Address")
    )

    rating = clean(
        row.get("Rating")
    )

    reviews = clean(
        row.get("Reviews")
    )

    linkedin = clean(
        row.get("LinkedIn")
    )

    facebook = clean(
        row.get("Facebook")
    )

    instagram = clean(
        row.get("Instagram")
    )

    maps_url = clean(
        row.get(
            "Google Maps URL"
        )
    )

    category = clean(
        row.get("Category")
    )

    hours = clean(
        row.get("Hours")
    )

    notes_parts = []

    if category:
        notes_parts.append(
            f"Category: {category}"
        )

    if hours:
        notes_parts.append(
            f"Hours: {hours}"
        )

    if facebook:
        notes_parts.append(
            f"Facebook: {facebook}"
        )

    if instagram:
        notes_parts.append(
            f"Instagram: {instagram}"
        )

    source_links = []

    for link in [
        maps_url,
        website,
        linkedin,
        facebook,
        instagram,
    ]:

        if (
            link
            and link
            not in source_links
        ):
            source_links.append(
                link
            )

    return {
        "name": name,

        "keyword": keyword,

        "city": (
            clean(
                row.get("City")
            )
            or fallback_city
        ),

        "state": (
            clean(
                row.get("State")
            )
            or fallback_state
        ),

        "country": country,

        "phone": phone,

        "website": website,

        "email": email,

        "address": address,

        "rating": rating,

        "reviews": reviews,

        "source": "Google Maps",

        "sources": "Google Maps",

        "linkedin": linkedin,

        "facebook": facebook,

        "instagram": instagram,

        "notes": "\n".join(
            notes_parts
        ),

        "source_links": (
            source_links
        ),

        "google_maps_url": (
            maps_url
        ),

        "latitude": clean(
            row.get("Latitude")
        ),

        "longitude": clean(
            row.get("Longitude")
        ),
    }


# ---------------------------------------------------------
# DEDUP KEY
# ---------------------------------------------------------

def _lead_key(
    lead,
):

    maps_url = clean(
        lead.get(
            "google_maps_url"
        )
    )

    if maps_url:
        return (
            "maps:"
            + maps_url
        )

    phone = re.sub(
        r"\D+",
        "",
        clean(
            lead.get("phone")
        ),
    )

    if phone:
        return (
            "phone:"
            + phone
        )

    name = re.sub(
        r"[^a-z0-9]+",
        "",
        clean(
            lead.get("name")
        ).lower(),
    )

    address = re.sub(
        r"[^a-z0-9]+",
        "",
        clean(
            lead.get("address")
        ).lower(),
    )

    if name:
        return (
            f"name:{name}|{address}"
        )

    return ""


# ---------------------------------------------------------
# CONSOLE -> PROGRESS
# ---------------------------------------------------------

def _parse_progress_line(
    line,
):

    text = str(
        line or ""
    ).strip()

    lower = (
        text.lower()
    )

    if not text:
        return 5, ""

    if "resolving" in lower:
        return (
            8,
            "Resolving location...",
        )

    if "resolved" in lower:
        return (
            14,
            "Location resolved",
        )

    if "planned" in lower:
        return (
            18,
            "Preparing search...",
        )

    if "searching:" in lower:
        return (
            22,
            "Searching Google Maps...",
        )

    if (
        "total results loaded"
        in lower
    ):

        match = re.search(
            r"total results loaded:\s*(\d+)",
            lower,
        )

        if match:

            return (
                34,
                (
                    f"Loaded "
                    f"{match.group(1)} "
                    f"businesses"
                ),
            )

        return (
            34,
            "Businesses loaded",
        )

    if (
        "found" in lower
        and "result cards"
        in lower
    ):

        match = re.search(
            r"found\s+(\d+)\s+result",
            lower,
        )

        if match:

            return (
                40,
                (
                    f"Opening "
                    f"{match.group(1)} "
                    f"business listings..."
                ),
            )

        return (
            40,
            "Opening listings...",
        )

    if (
        "extracted" in lower
        and "business" in lower
    ):

        match = re.search(
            r"extracted\s+(\d+)",
            lower,
        )

        if match:

            return (
                90,
                (
                    f"Extracted "
                    f"{match.group(1)} "
                    f"businesses"
                ),
            )

        return (
            90,
            "Businesses extracted",
        )

    if "browser closed" in lower:

        return (
            94,
            "Closing Google Maps browser...",
        )

    if "run complete" in lower:

        return (
            97,
            "Google Maps complete",
        )

    return (
        25,
        text[:120],
    )


# ---------------------------------------------------------
# TEXT CLEANING
# ---------------------------------------------------------

def clean(
    value,
):

    if value is None:
        return ""

    value = str(
        value
    ).strip()

    replacements = {
        "ÔÇ»": " ",
        "â€“": "-",
        "â€”": "-",
        "â€™": "'",
        "â€œ": '"',
        "â€\x9d": '"',
    }

    for bad, good in replacements.items():

        value = value.replace(
            bad,
            good,
        )

    return value