#!/usr/bin/env python3
"""
NEXUS Sheet Rebuild v2 - writes data + formatting in a single updateCells request per sheet.
Uses gspread for sheet management, raw batchUpdate API for cell data+format together.
Terminology: Verifications (not Assertions), Test Scripts (not Test Files), Pre-conditions.
"""

import os, sys, json, time, warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings('ignore')

# Add scripts dir to path so we can import nexus_scan
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nexus_scan import (
    scan_repo, post_process_modules, WEB_REPO, MOBILE_REPO, CREDS_PATH, SHEET_ID,
    TYPE_COLORS, HEADER_COLOR, TOTAL_COLOR
)

# ==================== COLOR HELPERS ====================

def rgb_dict(r, g, b):
    return {'red': r, 'green': g, 'blue': b}

# Theme colors
TITLE_BG = rgb_dict(0.118, 0.161, 0.231)       # #1E293B slate
TITLE_FG = rgb_dict(0.231, 0.510, 0.961)       # #3B82F6 blue
SUBTITLE_FG = rgb_dict(0.580, 0.639, 0.722)    # #94A3B8 gray
HEADER_BG = rgb_dict(0.059, 0.090, 0.165)      # #0F172A dark navy
HEADER_FG = rgb_dict(1.0, 1.0, 1.0)            # white
ALT_ROW_BG = rgb_dict(0.945, 0.961, 0.976)     # #F1F5F9 light gray
TOTAL_BG = rgb_dict(0.118, 0.161, 0.231)       # #1E293B slate
TOTAL_FG = rgb_dict(0.231, 0.510, 0.961)       # #3B82F6 blue
BLUE_BORDER = rgb_dict(0.231, 0.510, 0.961)    # #3B82F6

TYPE_FORMAT_MAP = {
    'assertEquals':      {'bg': rgb_dict(0.820, 0.980, 0.898), 'fg': rgb_dict(0.024, 0.373, 0.275)},
    'assertTrue':        {'bg': rgb_dict(0.820, 0.980, 0.898), 'fg': rgb_dict(0.024, 0.373, 0.275)},
    'assertFalse':       {'bg': rgb_dict(0.820, 0.980, 0.898), 'fg': rgb_dict(0.024, 0.373, 0.275)},
    'isElementPresent':  {'bg': rgb_dict(0.859, 0.922, 0.980), 'fg': rgb_dict(0.118, 0.251, 0.686)},
    'verifyElementText': {'bg': rgb_dict(0.812, 0.984, 0.984), 'fg': rgb_dict(0.082, 0.369, 0.459)},
    'waitForElement':    {'bg': rgb_dict(0.996, 0.953, 0.780), 'fg': rgb_dict(0.573, 0.251, 0.055)},
    'Pre-condition':     {'bg': rgb_dict(0.930, 0.930, 0.930), 'fg': rgb_dict(0.450, 0.450, 0.450)},
    'Pre-condition (waitForElement)': {'bg': rgb_dict(0.930, 0.930, 0.930), 'fg': rgb_dict(0.450, 0.450, 0.450)},
}
# Note: Assert.fail is excluded by nexus_scan.py post_process_modules() and should never appear in output

# ==================== HEADER COMMENTS (hover notes) ====================

HEADER_COMMENTS = {
    # Grand Summary
    'Platform': 'Web, Android, or iOS',
    'Modules': 'Number of test module folders detected in the repo',
    'Test Scripts': 'Number of .java test files in this module',
    'Verifications': 'Count of actual test checks. Excludes pre-conditions (waitForElement) and error handlers (Assert.fail)',
    'Pre-conditions': 'waitForElement/waitForVisibility calls. These are waits before interactions, not actual test checks',
    'Total': 'Verifications + Pre-conditions combined',
    'Prod Suites': 'Verifications found in production/sanity smoke test suites',

    # Platform Summaries
    'Module': 'Test module name, auto-detected from folder structure (e.g. tests/Web/Cards/ → Cards)',
    'isElementPresent': 'Checks that a UI element is displayed on screen using isDisplayed()',
    'assertEquals': 'Compares expected value with actual value — the strongest type of verification',
    'verifyElementText': 'Confirms the text content of a UI element matches the expected string',
    'assertTrue/False': 'Boolean condition checks (e.g., button is visible, flag is set)',
    '% of Total': "This module's share of the platform's total verifications",
    'Other': 'Verification types that do not fall into the standard categories',

    # Detail Sheets
    'Test Script': 'The .java test file containing this verification',
    'Number': 'Sequential row number within this sheet',
    'Verification': 'Human-readable description of what is being checked',
    'Type': 'The Java assertion/verification method used (isElementPresent, assertEquals, verifyElementText, assertTrue/assertFalse)',

    # Verification Types
    'Web': 'Count from Novo-P1-UI-Tests repository (Selenium web tests)',
    'Android': 'Count from Novo_Mobile_UIAutomation_Appium repository (Android Appium tests)',
    'iOS': 'Count from Novo_Mobile_UIAutomation_Appium repository (iOS Appium tests)',
    'Prod': 'Count from production/sanity smoke test suites',

    # Production Suites
    'Suite': 'Name of the production smoke/sanity test suite',
}

# ==================== CELL BUILDER ====================

def make_cell(value, bg=None, fg=None, bold=False, font_size=10, halign='LEFT',
              font_family='Inter', border_bottom=None, border_top=None, note=None):
    """Build a CellData dict with both userEnteredValue and userEnteredFormat."""
    cell = {}

    # Value
    if value is None or value == '':
        cell['userEnteredValue'] = {'stringValue': ''}
    elif isinstance(value, (int, float)):
        cell['userEnteredValue'] = {'numberValue': value}
    else:
        cell['userEnteredValue'] = {'stringValue': str(value)}

    # Format
    fmt = {
        'textFormat': {
            'bold': bold,
            'fontSize': font_size,
            'fontFamily': font_family,
        },
        'horizontalAlignment': halign,
        'verticalAlignment': 'MIDDLE',
    }
    if fg:
        fmt['textFormat']['foregroundColorStyle'] = {'rgbColor': fg}
    if bg:
        fmt['backgroundColorStyle'] = {'rgbColor': bg}

    if border_bottom:
        fmt['borders'] = {'bottom': {'style': 'SOLID', 'width': 2, 'colorStyle': {'rgbColor': border_bottom}}}
    if border_top:
        fmt.setdefault('borders', {})['top'] = {'style': 'SOLID', 'width': 2, 'colorStyle': {'rgbColor': border_top}}

    cell['userEnteredFormat'] = fmt

    # Note (hover comment)
    if note:
        cell['note'] = note

    return cell


def title_cell(value, num_cols=1):
    """Title row cell."""
    return make_cell(value, bg=TITLE_BG, fg=TITLE_FG, bold=True, font_size=14)

def subtitle_cell(value):
    """Subtitle/timestamp cell."""
    return make_cell(value, fg=SUBTITLE_FG, font_size=10)

def header_cell(value):
    """Header row cell. Automatically adds hover note from HEADER_COMMENTS if available."""
    note = HEADER_COMMENTS.get(value)
    return make_cell(value, bg=HEADER_BG, fg=HEADER_FG, bold=True, font_size=10,
                     halign='CENTER', border_bottom=BLUE_BORDER, note=note)

def data_cell(value, alt=False, halign='LEFT', type_val=None):
    """Data row cell. If type_val provided and matches, apply type color coding."""
    bg = ALT_ROW_BG if alt else None
    fg = None
    bold = False

    if type_val and type_val in TYPE_FORMAT_MAP:
        bg = TYPE_FORMAT_MAP[type_val]['bg']
        fg = TYPE_FORMAT_MAP[type_val]['fg']
        bold = True

    if isinstance(value, (int, float)):
        halign = 'RIGHT'

    return make_cell(value, bg=bg, fg=fg, bold=bold, font_size=10, halign=halign)

def total_cell(value):
    """Total row cell."""
    halign = 'RIGHT' if isinstance(value, (int, float)) else 'LEFT'
    return make_cell(value, bg=TOTAL_BG, fg=TOTAL_FG, bold=True, font_size=11,
                     border_top=BLUE_BORDER)

def empty_cell():
    return make_cell('')

# Verification count color bars: green (>100), blue (50-99), amber (20-49), gray (<20)
VERIF_COLOR_BARS = {
    'green': {'bg': rgb_dict(0.820, 0.980, 0.898), 'fg': rgb_dict(0.024, 0.373, 0.275)},
    'blue':  {'bg': rgb_dict(0.859, 0.922, 0.980), 'fg': rgb_dict(0.118, 0.251, 0.686)},
    'amber': {'bg': rgb_dict(0.996, 0.953, 0.780), 'fg': rgb_dict(0.573, 0.251, 0.055)},
    'gray':  {'bg': rgb_dict(0.930, 0.930, 0.930), 'fg': rgb_dict(0.450, 0.450, 0.450)},
}

def verif_count_cell(count, alt=False):
    """Data cell with color bar based on verification count."""
    if count >= 100:
        colors = VERIF_COLOR_BARS['green']
    elif count >= 50:
        colors = VERIF_COLOR_BARS['blue']
    elif count >= 20:
        colors = VERIF_COLOR_BARS['amber']
    else:
        colors = VERIF_COLOR_BARS['gray']
    return make_cell(count, bg=colors['bg'], fg=colors['fg'], bold=True, font_size=10, halign='RIGHT')

# ==================== BUILD ROW HELPERS ====================

def build_title_row(text, num_cols):
    return [title_cell(text)] + [title_cell('') for _ in range(num_cols - 1)]

def build_subtitle_row(text, num_cols):
    return [subtitle_cell(text)] + [subtitle_cell('') for _ in range(num_cols - 1)]

def build_header_row(headers):
    return [header_cell(h) for h in headers]

def build_data_row(values, alt=False, type_col=None):
    """Build a data row. type_col is the column index that has type-based coloring."""
    type_val = None
    if type_col is not None and type_col < len(values):
        type_val = values[type_col] if isinstance(values[type_col], str) else None

    cells = []
    for i, v in enumerate(values):
        if type_col is not None and i == type_col:
            cells.append(data_cell(v, alt=alt, type_val=type_val))
        else:
            cells.append(data_cell(v, alt=alt))
    return cells

def build_total_row(values):
    return [total_cell(v) for v in values]

def build_empty_row(num_cols):
    return [empty_cell() for _ in range(num_cols)]

# ==================== SHEET WRITING ====================

def write_sheet_data(spreadsheet, ws, all_rows, col_widths, freeze_rows, chunk_size=200):
    """Write all_rows to ws using updateCells in chunks, then set col widths and freeze."""
    sheet_id = ws.id
    total_rows = len(all_rows)
    num_cols = max(len(r) for r in all_rows) if all_rows else 1

    # Pad rows to same length
    for row in all_rows:
        while len(row) < num_cols:
            row.append(empty_cell())

    requests = []

    # Write data+format in chunks
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk_rows = all_rows[start:end]

        rows_data = []
        for row in chunk_rows:
            rows_data.append({'values': row})

        requests.append({
            'updateCells': {
                'rows': rows_data,
                'fields': 'userEnteredValue,userEnteredFormat,note',
                'start': {
                    'sheetId': sheet_id,
                    'rowIndex': start,
                    'columnIndex': 0
                }
            }
        })

    # Column widths
    if col_widths:
        for i, w in enumerate(col_widths):
            if i < num_cols:
                requests.append({
                    'updateDimensionProperties': {
                        'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS',
                                  'startIndex': i, 'endIndex': i + 1},
                        'properties': {'pixelSize': w},
                        'fields': 'pixelSize'
                    }
                })

    # Freeze rows
    if freeze_rows:
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {'frozenRowCount': freeze_rows}
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })

    # Header row height (34px) - find header rows (freeze_rows is typically the header row)
    if freeze_rows:
        requests.append({
            'updateDimensionProperties': {
                'range': {'sheetId': sheet_id, 'dimension': 'ROWS',
                          'startIndex': freeze_rows - 1, 'endIndex': freeze_rows},
                'properties': {'pixelSize': 34},
                'fields': 'pixelSize'
            }
        })

    # Send in batches of ~20 requests to stay under API limits
    batch_size = 30
    for i in range(0, len(requests), batch_size):
        batch = requests[i:i + batch_size]
        spreadsheet.batch_update({'requests': batch})
        if i + batch_size < len(requests):
            time.sleep(0.5)


def get_or_create(ss, title, rows=2000, cols=20):
    try:
        return ss.worksheet(title)
    except:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


# ==================== SHEET BUILDERS ====================

def _count_verifications(data):
    return sum(len(m['assertions']) for m in data.values())

def _count_preconditions(data):
    return sum(len(m.get('preconditions', [])) for m in data.values())

def build_grand_summary(web, android, ios, android_prod, ios_prod, ts):
    """Build all rows for Grand Summary sheet."""
    web_v = _count_verifications(web)
    and_v = _count_verifications(android)
    ios_v = _count_verifications(ios)
    andp_v = _count_verifications(android_prod)
    iosp_v = _count_verifications(ios_prod)
    web_p = _count_preconditions(web)
    and_p = _count_preconditions(android)
    ios_p = _count_preconditions(ios)
    andp_p = _count_preconditions(android_prod)
    iosp_p = _count_preconditions(ios_prod)
    grand_v = web_v + and_v + ios_v + andp_v + iosp_v
    grand_p = web_p + and_p + ios_p + andp_p + iosp_p

    NC = 7  # num cols (added Pre-conditions)
    rows = []
    # Row 1: Title
    rows.append(build_title_row('NEXUS Verification Registry', NC))
    # Row 2: Timestamp
    rows.append(build_subtitle_row(f'Last scanned: {ts}', NC))
    # Row 3: blank
    rows.append(build_empty_row(NC))
    # Row 4: Header
    rows.append(build_header_row(['Platform', 'Modules', 'Test Scripts', 'Verifications', 'Pre-conditions', 'Total', 'Prod Suites']))
    # Rows 5-7: Platform data
    rows.append(build_data_row(['Web', len(web), sum(len(m['files']) for m in web.values()), web_v, web_p, web_v + web_p, 0], alt=False))
    rows.append(build_data_row(['Android', len(android), sum(len(m['files']) for m in android.values()), and_v, and_p, and_v + and_p, andp_v], alt=True))
    rows.append(build_data_row(['iOS', len(ios), sum(len(m['files']) for m in ios.values()), ios_v, ios_p, ios_v + ios_p, iosp_v], alt=False))
    # Row 8: TOTAL
    rows.append(build_total_row(['TOTAL', len(web)+len(android)+len(ios),
                                  sum(len(m['files']) for d in [web,android,ios] for m in d.values()),
                                  web_v+and_v+ios_v, web_p+and_p+ios_p, grand_v+grand_p, andp_v+iosp_v]))
    # Row 9: blank
    rows.append(build_empty_row(NC))

    # Row 10: Verification Types header
    vt_start_idx = len(rows)  # track for chart data source
    rows.append(build_header_row(['Verification Types', 'Web', 'Android', 'iOS', 'Prod', 'Total', '']))
    all_types = sorted(set(t for d in [web,android,ios,android_prod,ios_prod] for m in d.values() for t in m['types']))
    all_types = [t for t in all_types if t != 'Assert.fail']  # Safety: exclude Assert.fail
    # Rename 'waitForElement' display and handle Pre-condition
    for i, t in enumerate(all_types):
        display_name = 'Pre-condition (waitForElement)' if t == 'Pre-condition' else t
        wc = sum(m['types'].get(t,0) for m in web.values())
        ac = sum(m['types'].get(t,0) for m in android.values())
        ic = sum(m['types'].get(t,0) for m in ios.values())
        pc = sum(m['types'].get(t,0) for d in [android_prod,ios_prod] for m in d.values())
        rows.append(build_data_row([display_name, wc, ac, ic, pc, wc+ac+ic+pc, ''], alt=(i % 2 == 1), type_col=0))
    rows.append(build_total_row(['TOTAL', web_v+web_p, and_v+and_p, ios_v+ios_p, andp_v+iosp_v+andp_p+iosp_p, grand_v+grand_p, '']))

    # Record where the Verification Types table starts (header row) and ends (TOTAL row)
    # vt_header_row is the row index of the "Verification Types" header
    # vt_total_row is the row index of the TOTAL row (last row)
    vt_header_row = vt_start_idx
    vt_total_row = len(rows) - 1

    col_widths = [160, 100, 110, 120, 130, 100, 120]
    return rows, col_widths, 4, vt_header_row, vt_total_row  # freeze_rows=4


def build_platform_summary(name, data, ts):
    """Build rows for a platform summary sheet."""
    headers = ['Module', 'Test Scripts', 'Verifications', 'Pre-conditions', 'isElementPresent', 'assertEquals',
               'verifyElementText', 'assertTrue/False', 'Other', '% of Total']
    NC = len(headers)
    rows = []
    rows.append(build_title_row(name, NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    # Compute total verifications for % of Total
    total_verifs = sum(len(m['assertions']) for m in data.values())

    data_rows = []
    for mn in sorted(data.keys()):
        m = data[mn]; ty = m['types']
        verif_count = len(m['assertions'])
        precond_count = len(m.get('preconditions', []))
        pct = round(verif_count / total_verifs * 100, 1) if total_verifs > 0 else 0
        data_rows.append([
            mn, len(m['files']), verif_count, precond_count,
            ty.get('isElementPresent', 0), ty.get('assertEquals', 0),
            ty.get('verifyElementText', 0),
            ty.get('assertTrue', 0) + ty.get('assertFalse', 0),
            sum(v for k, v in ty.items() if k not in ('isElementPresent', 'assertEquals', 'verifyElementText', 'Pre-condition', 'assertTrue', 'assertFalse', 'Assert.fail')),
            f'{pct}%'
        ])
    for i, dr in enumerate(data_rows):
        # Build row but replace verifications cell with color-bar version
        row_cells = []
        for j, v in enumerate(dr):
            if j == 2:  # Verifications column
                row_cells.append(verif_count_cell(v, alt=(i % 2 == 1)))
            else:
                row_cells.append(data_cell(v, alt=(i % 2 == 1)))
        rows.append(row_cells)

    # Totals
    total_precond = sum(len(m.get('preconditions', [])) for m in data.values())
    totals = ['TOTAL', sum(len(m['files']) for m in data.values()), total_verifs, total_precond]
    for c in range(4, 9):
        totals.append(sum(dr[c] for dr in data_rows if len(dr) > c and isinstance(dr[c], (int, float))))
    totals.append('100%')
    rows.append(build_total_row(totals))

    col_widths = [180, 100, 120, 120, 130, 110, 130, 120, 80, 90]
    return rows, col_widths, 3


def build_detail_sheet(sheet_name, data, ts, max_rows=None):
    """Build rows for a detail verifications sheet. Pre-conditions are excluded."""
    headers = ['Test Script', 'Module', 'Number', 'Verification', 'Type']
    NC = len(headers)
    rows = []
    rows.append(build_title_row(sheet_name, NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    num = 1
    for mn in sorted(data.keys()):
        # Only show actual verifications — exclude pre-conditions
        for a in data[mn].get('assertions', []):
            if a.get('type') == 'Pre-condition':
                continue  # Skip pre-conditions in detail sheets
            if a.get('type') == 'Assert.fail':
                continue  # Safety: exclude Assert.fail
            # Build row with bold description
            row_cells = [
                data_cell(a.get('file', ''), alt=((num - 1) % 2 == 1)),
                data_cell(mn, alt=((num - 1) % 2 == 1)),
                data_cell(num, alt=((num - 1) % 2 == 1)),
                make_cell(a['description'], bold=True, font_size=10,
                          bg=ALT_ROW_BG if (num - 1) % 2 == 1 else None),
                data_cell(a['type'], alt=((num - 1) % 2 == 1), type_val=a['type']),
            ]
            rows.append(row_cells)
            num += 1
            if max_rows and num - 1 >= max_rows:
                break
        if max_rows and num - 1 >= max_rows:
            break

    col_widths = [250, 150, 80, 400, 140]
    return rows, col_widths, 3


def build_prod_suites(android_prod, ios_prod, ts):
    """Build rows for Production Suites sheet."""
    headers = ['Suite', 'Platform', 'Test Scripts', 'Verifications']
    NC = len(headers)
    rows = []
    rows.append(build_title_row('Production Suites', NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    i = 0
    for mn, m in sorted(android_prod.items()):
        rows.append(build_data_row([mn, 'Android', len(m['files']), len(m['assertions'])], alt=(i % 2 == 1)))
        i += 1
    for mn, m in sorted(ios_prod.items()):
        rows.append(build_data_row([mn, 'iOS', len(m['files']), len(m['assertions'])], alt=(i % 2 == 1)))
        i += 1

    andp_t = sum(len(m['assertions']) for m in android_prod.values())
    iosp_t = sum(len(m['assertions']) for m in ios_prod.values())
    rows.append(build_total_row(['TOTAL', '',
                                  sum(len(m['files']) for d in [android_prod, ios_prod] for m in d.values()),
                                  andp_t + iosp_t]))

    col_widths = [250, 120, 100, 120]
    return rows, col_widths, 3


def build_verification_types(web, android, ios, android_prod, ios_prod, ts):
    """Build rows for Verification Types sheet."""
    headers = ['Type', 'Web', 'Android', 'iOS', 'Prod', 'Total']
    NC = len(headers)
    rows = []
    rows.append(build_title_row('Verification Types Cross-Platform', NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    web_t = _count_verifications(web) + _count_preconditions(web)
    and_t = _count_verifications(android) + _count_preconditions(android)
    ios_t = _count_verifications(ios) + _count_preconditions(ios)
    andp_t = _count_verifications(android_prod) + _count_preconditions(android_prod)
    iosp_t = _count_verifications(ios_prod) + _count_preconditions(ios_prod)
    grand = web_t + and_t + ios_t + andp_t + iosp_t

    all_types = sorted(set(t for d in [web, android, ios, android_prod, ios_prod] for m in d.values() for t in m['types']))
    all_types = [t for t in all_types if t != 'Assert.fail']  # Safety: exclude Assert.fail
    for i, t in enumerate(all_types):
        display_name = 'Pre-condition (waitForElement)' if t == 'Pre-condition' else t
        wc = sum(m['types'].get(t, 0) for m in web.values())
        ac = sum(m['types'].get(t, 0) for m in android.values())
        ic = sum(m['types'].get(t, 0) for m in ios.values())
        pc = sum(m['types'].get(t, 0) for d in [android_prod, ios_prod] for m in d.values())
        rows.append(build_data_row([display_name, wc, ac, ic, pc, wc+ac+ic+pc], alt=(i % 2 == 1), type_col=0))

    rows.append(build_total_row(['TOTAL', web_t, and_t, ios_t, andp_t + iosp_t, grand]))

    col_widths = [200, 100, 100, 100, 100, 100]
    return rows, col_widths, 3


# ==================== CHARTS ====================

def add_charts(spreadsheet, ws, vt_header_row, vt_total_row):
    """Add a single stacked bar chart using the Verification Types table in Grand Summary.
    vt_header_row: row index of the Verification Types header row
    vt_total_row: row index of the TOTAL row at the end of the table
    The data rows are between header+1 and total (exclusive).
    Columns: 0=Type, 1=Web, 2=Android, 3=iOS, 4=Prod
    """
    sid = ws.id
    try:
        # Delete existing charts
        meta = spreadsheet.fetch_sheet_metadata()
        existing_charts = []
        for s in meta.get('sheets', []):
            if s['properties']['sheetId'] == sid:
                existing_charts = [c['chartId'] for c in s.get('charts', [])]
        for cid in existing_charts:
            spreadsheet.batch_update({'requests': [{'deleteEmbeddedObject': {'objectId': cid}}]})

        # Data rows are from vt_header_row+1 to vt_total_row (exclusive of TOTAL)
        data_start = vt_header_row + 1
        data_end = vt_total_row  # exclusive — don't include the TOTAL row

        # Stacked bar chart: Y-axis = verification types, segments = platforms (Web, Android, iOS, Prod)
        # Column 0 = Type names (domain), Columns 1-4 = Web, Android, iOS, Prod (series)
        series_colors = [
            {'red': 0.23, 'green': 0.51, 'blue': 0.96},  # Web - blue
            {'red': 0.16, 'green': 0.71, 'blue': 0.36},   # Android - green
            {'red': 0.96, 'green': 0.62, 'blue': 0.04},   # iOS - amber
            {'red': 0.61, 'green': 0.15, 'blue': 0.69},   # Prod - purple
        ]
        platform_cols = [1, 2, 3, 4]  # Web, Android, iOS, Prod
        platform_names = ['Web', 'Android', 'iOS', 'Prod']

        series = []
        for idx, col in enumerate(platform_cols):
            series.append({
                'series': {
                    'sourceRange': {
                        'sources': [{
                            'sheetId': sid,
                            'startRowIndex': data_start,
                            'endRowIndex': data_end,
                            'startColumnIndex': col,
                            'endColumnIndex': col + 1
                        }]
                    }
                },
                'color': series_colors[idx],
            })

        requests = [
            {'addChart': {'chart': {
                'spec': {
                    'title': 'Verifications by Platform and Type',
                    'basicChart': {
                        'chartType': 'BAR',
                        'legendPosition': 'BOTTOM_LEGEND',
                        'stackedType': 'STACKED',
                        'headerCount': 0,
                        'axis': [
                            {'position': 'BOTTOM_AXIS', 'title': 'Count'},
                            {'position': 'LEFT_AXIS', 'title': ''}
                        ],
                        'domains': [{
                            'domain': {
                                'sourceRange': {
                                    'sources': [{
                                        'sheetId': sid,
                                        'startRowIndex': data_start,
                                        'endRowIndex': data_end,
                                        'startColumnIndex': 0,
                                        'endColumnIndex': 1
                                    }]
                                }
                            }
                        }],
                        'series': series,
                    }
                },
                'position': {'overlayPosition': {
                    'anchorCell': {'sheetId': sid, 'rowIndex': 0, 'columnIndex': 7},
                    'widthPixels': 600,
                    'heightPixels': 400
                }}
            }}}
        ]
        spreadsheet.batch_update({'requests': requests})
        print('  Stacked bar chart added successfully')
    except Exception as e:
        print(f'  Chart warning: {e}')


# ==================== CLEAN EXISTING SHEETS ====================

def clean_spreadsheet(ss):
    """Delete all sheets except the first one, clear the first one."""
    sheets = ss.worksheets()
    # Keep the first sheet, rename it
    first = sheets[0]
    if first.title != 'Grand Summary':
        first.update_title('Grand Summary')
    first.clear()
    # Resize first sheet
    first.resize(rows=2000, cols=20)

    # Delete all other sheets
    for ws in sheets[1:]:
        try:
            ss.del_worksheet(ws)
            time.sleep(0.3)
        except Exception as e:
            print(f'  Warning deleting {ws.title}: {e}')

    return first


# ==================== MAIN ====================

def main():
    start_time = time.time()
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print('=' * 60)
    print('NEXUS Sheet Rebuild v2 - Verifications + Pre-conditions')
    print('=' * 60)
    print(f'Time: {ts}')

    # Step 1: Scan repos and post-process
    print('\n[1/4] Scanning repositories...')
    print('  Scanning Web repo...')
    web = scan_repo(WEB_REPO, 'src/test/java/tests', 'src/main/java/pages', 'web')
    web = post_process_modules(web)
    for n in sorted(web):
        print(f'    {n}: {len(web[n]["files"])} scripts, {len(web[n]["assertions"])} verifications, {len(web[n].get("preconditions",[]))} pre-conditions')

    print('  Scanning Android...')
    android = scan_repo(MOBILE_REPO, 'src/test/java/tests/android', 'src/main/java/pages/android', 'android')
    android = post_process_modules(android)
    for n in sorted(android):
        print(f'    {n}: {len(android[n]["files"])} scripts, {len(android[n]["assertions"])} verifications, {len(android[n].get("preconditions",[]))} pre-conditions')

    print('  Scanning iOS...')
    ios = scan_repo(MOBILE_REPO, 'src/test/java/tests/iOS', 'src/main/java/pages/iOS', 'ios')
    ios = post_process_modules(ios)
    for n in sorted(ios):
        print(f'    {n}: {len(ios[n]["files"])} scripts, {len(ios[n]["assertions"])} verifications, {len(ios[n].get("preconditions",[]))} pre-conditions')

    print('  Scanning Prod suites...')
    android_prod = scan_repo(MOBILE_REPO, 'src/test/java/tests/androidProdSanitySuite', 'src/main/java/pages/android', 'android')
    android_prod = post_process_modules(android_prod)
    ios_prod = scan_repo(MOBILE_REPO, 'src/test/java/tests/iOSProdSuite', 'src/main/java/pages/iOS', 'ios')
    ios_prod = post_process_modules(ios_prod)
    print(f'    Android Prod: {sum(len(m["assertions"]) for m in android_prod.values())} verifications')
    print(f'    iOS Prod: {sum(len(m["assertions"]) for m in ios_prod.values())} verifications')

    total_v = sum(len(m['assertions']) for d in [web, android, ios, android_prod, ios_prod] for m in d.values())
    total_p = sum(len(m.get('preconditions', [])) for d in [web, android, ios, android_prod, ios_prod] for m in d.values())
    print(f'\n  GRAND TOTAL: {total_v} verifications + {total_p} pre-conditions')

    # Step 2: Connect to Google Sheets and clean
    print('\n[2/4] Connecting to Google Sheets...')
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_PATH, scope)
    client = gspread.authorize(creds)
    ss = client.open_by_key(SHEET_ID)
    print('  Connected.')

    print('  Cleaning existing sheets...')
    grand_ws = clean_spreadsheet(ss)
    time.sleep(1)
    print('  Cleaned.')

    # Step 3: Build and write all sheets
    print('\n[3/4] Building and writing sheets with data + formatting...')

    # --- Grand Summary ---
    print('  1/12 Grand Summary...')
    gs_rows, gs_widths, gs_freeze, vt_header_row, vt_total_row = build_grand_summary(web, android, ios, android_prod, ios_prod, ts)
    write_sheet_data(ss, grand_ws, gs_rows, gs_widths, gs_freeze)
    print(f'    {len(gs_rows)} rows written')
    time.sleep(1)

    # --- Web Summary ---
    print('  2/12 Web Summary...')
    ws = get_or_create(ss, 'Web Summary')
    r, cw, fr = build_platform_summary('Web Summary', web, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- Android Summary ---
    print('  3/12 Android Summary...')
    ws = get_or_create(ss, 'Android Summary')
    r, cw, fr = build_platform_summary('Android Summary', android, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- iOS Summary ---
    print('  4/12 iOS Summary...')
    ws = get_or_create(ss, 'iOS Summary')
    r, cw, fr = build_platform_summary('iOS Summary', ios, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- Production Suites ---
    print('  5/12 Production Suites...')
    ws = get_or_create(ss, 'Production Suites')
    r, cw, fr = build_prod_suites(android_prod, ios_prod, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- Detail sheets ---
    detail_configs = [
        ('Web - Cards', {'Cards': web.get('Cards', {'assertions': [], 'files': set(), 'types': {}})}, 200),
        ('Web - Checking', {'Checking': web.get('Checking', {'assertions': [], 'files': set(), 'types': {}})}, None),
        ('Web - Invoices', {'Invoices': web.get('Invoices', {'assertions': [], 'files': set(), 'types': {}})}, None),
        ('Android - All', android, 300),
        ('iOS - All', ios, 300),
        ('Production - All', {**android_prod, **ios_prod}, 200),
    ]

    for idx, (sheet_name, data, max_rows) in enumerate(detail_configs):
        sheet_num = 6 + idx
        print(f'  {sheet_num}/12 {sheet_name}...')
        ws = get_or_create(ss, sheet_name)
        r, cw, fr = build_detail_sheet(sheet_name, data, ts, max_rows=max_rows)
        write_sheet_data(ss, ws, r, cw, fr)
        print(f'    {len(r)} rows written')
        time.sleep(0.5)

    # --- Verification Types ---
    print('  12/12 Verification Types...')
    ws = get_or_create(ss, 'Verification Types')
    r, cw, fr = build_verification_types(web, android, ios, android_prod, ios_prod, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # Step 4: Charts
    print('\n[4/4] Adding charts to Grand Summary...')
    grand_ws = ss.worksheet('Grand Summary')
    add_charts(ss, grand_ws, vt_header_row, vt_total_row)

    elapsed = time.time() - start_time
    print(f'\n{"=" * 60}')
    print(f'DONE! All 12 sheets rebuilt with verifications + pre-conditions in {elapsed:.1f}s')
    print(f'Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}')
    print(f'{"=" * 60}')


if __name__ == '__main__':
    main()
