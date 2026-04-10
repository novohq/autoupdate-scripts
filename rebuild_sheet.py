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

# ==================== EXECUTIVE DASHBOARD CONSTANTS ====================

MANUAL_TIME_PER_VERIFICATION_MINS = 2.5
CI_TIMES_HOURS = {'Web': 4, 'Android': 3, 'iOS': 2}
TOTAL_KNOWN_MODULES = 15  # denominator for maturity calculation

# Dashboard colors
DARK_BG = rgb_dict(0.118, 0.161, 0.231)           # #1E293B
KPI_BLUE = rgb_dict(0.231, 0.510, 0.961)          # #3B82F6
KPI_GREEN = rgb_dict(0.063, 0.725, 0.506)         # #10B981
KPI_PURPLE = rgb_dict(0.545, 0.361, 0.965)        # #8B5CF6
KPI_AMBER = rgb_dict(0.961, 0.620, 0.043)         # #F59E0B
LABEL_GRAY = rgb_dict(0.580, 0.639, 0.722)        # #94A3B8

# Heatmap backgrounds
HEATMAP_HIGH_BG = rgb_dict(0.820, 0.980, 0.898)   # #D1FAE5
HEATMAP_MED_BG = rgb_dict(0.859, 0.922, 0.980)    # #DBEAFE
HEATMAP_LOW_BG = rgb_dict(0.996, 0.953, 0.780)    # #FEF3C7
HEATMAP_NONE_BG = rgb_dict(0.996, 0.886, 0.886)   # #FEE2E2
HEATMAP_NONE_FG = rgb_dict(0.600, 0.600, 0.600)   # gray

# Maturity colors
MATURE_GREEN = rgb_dict(0.024, 0.573, 0.275)      # green text
GROWING_AMBER = rgb_dict(0.750, 0.500, 0.000)     # amber text
EARLY_PURPLE = rgb_dict(0.545, 0.361, 0.965)      # purple text

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
              font_family='Inter', border_bottom=None, border_top=None, border_left=None,
              valign='MIDDLE', wrap_strategy=None, note=None):
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
        'verticalAlignment': valign,
    }
    if fg:
        fmt['textFormat']['foregroundColorStyle'] = {'rgbColor': fg}
    if bg:
        fmt['backgroundColorStyle'] = {'rgbColor': bg}
    if wrap_strategy:
        fmt['wrapStrategy'] = wrap_strategy

    borders = {}
    if border_bottom:
        borders['bottom'] = {'style': 'SOLID', 'width': 2, 'colorStyle': {'rgbColor': border_bottom}}
    if border_top:
        borders['top'] = {'style': 'SOLID', 'width': 2, 'colorStyle': {'rgbColor': border_top}}
    if border_left:
        borders['left'] = {'style': 'SOLID', 'width': 3, 'colorStyle': {'rgbColor': border_left}}
    if borders:
        fmt['borders'] = borders

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

def _section_title_row(text, nc, note=None):
    """Build a section title row — big font, NO dark background, just bold blue text."""
    cells = [make_cell(text, fg=TITLE_FG, bold=True, font_size=16,
                       border_left=BLUE_BORDER, note=note)]
    cells += [empty_cell() for _ in range(nc - 1)]
    return cells


def build_grand_summary(web, android, ios, android_prod, ios_prod, ts):
    """Build all rows for Grand Summary sheet including executive dashboard."""
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

    NC = 8  # expanded to 8 columns for dashboard boxes
    merges = []  # collect merge requests
    rows = []

    # Row 1: Title
    rows.append(build_title_row('NEXUS Verification Registry', NC))
    # Row 2: Timestamp
    rows.append(build_subtitle_row(f'Last scanned: {ts}', NC))
    # Row 3: blank
    rows.append(build_empty_row(NC))
    # Row 4: Header
    rows.append(build_header_row(['Platform', 'Modules', 'Test Scripts', 'Verifications', 'Pre-conditions', 'Total', 'Prod Suites', '']))
    # Rows 5-7: Platform data
    web_scripts = sum(len(m['files']) for m in web.values())
    and_scripts = sum(len(m['files']) for m in android.values())
    ios_scripts = sum(len(m['files']) for m in ios.values())
    rows.append(build_data_row(['Web', len(web), web_scripts, web_v, web_p, web_v + web_p, 0, ''], alt=False))
    rows.append(build_data_row(['Android', len(android), and_scripts, and_v, and_p, and_v + and_p, andp_v, ''], alt=True))
    rows.append(build_data_row(['iOS', len(ios), ios_scripts, ios_v, ios_p, ios_v + ios_p, iosp_v, ''], alt=False))
    # Row 8: TOTAL
    total_scripts = web_scripts + and_scripts + ios_scripts
    total_modules = len(web) + len(android) + len(ios)
    prod_v = andp_v + iosp_v
    total_v_no_prod = web_v + and_v + ios_v
    total_p_no_prod = web_p + and_p + ios_p
    rows.append(build_total_row(['TOTAL', total_modules, total_scripts,
                                  total_v_no_prod, total_p_no_prod, total_v_no_prod + total_p_no_prod, prod_v, '']))
    # Row 9: blank
    rows.append(build_empty_row(NC))

    # Row 10: Verification Types header
    rows.append(build_header_row(['Verification Types', 'Web', 'Android', 'iOS', 'Prod', 'Total', '', '']))
    all_types = sorted(set(t for d in [web,android,ios,android_prod,ios_prod] for m in d.values() for t in m['types']))
    all_types = [t for t in all_types if t != 'Assert.fail']
    for i, t in enumerate(all_types):
        display_name = 'Pre-condition (waitForElement)' if t == 'Pre-condition' else t
        wc = sum(m['types'].get(t,0) for m in web.values())
        ac = sum(m['types'].get(t,0) for m in android.values())
        ic = sum(m['types'].get(t,0) for m in ios.values())
        pc = sum(m['types'].get(t,0) for d in [android_prod,ios_prod] for m in d.values())
        rows.append(build_data_row([display_name, wc, ac, ic, pc, wc+ac+ic+pc, '', ''], alt=(i % 2 == 1), type_col=0))
    rows.append(build_total_row(['TOTAL', web_v+web_p, and_v+and_p, ios_v+ios_p, andp_v+iosp_v+andp_p+iosp_p, grand_v+grand_p, '', '']))

    # ==================== EXECUTIVE DASHBOARD ====================
    # 2 blank rows after Verification Types table
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))

    # Computed values used across sections
    total_verifications = web_v + and_v + ios_v  # excludes prod
    platform_verifs = {'Web': web_v, 'Android': and_v, 'iOS': ios_v}
    platform_scripts = {'Web': web_scripts, 'Android': and_scripts, 'iOS': ios_scripts}
    platform_modules = {'Web': len(web), 'Android': len(android), 'iOS': len(ios)}

    # ---- SECTION 1: HERO KPIs ----
    print('    Section 1: Hero KPIs...')
    r = len(rows)
    # Number row (big numbers)
    rows.append([
        make_cell(f'{total_verifications:,}', bg=DARK_BG, fg=KPI_BLUE, bold=True, font_size=24, halign='CENTER',
                  note='Total actual test checks across Web + Android + iOS (excludes pre-conditions and error handlers)'),
        make_cell('', bg=DARK_BG),
        make_cell(f'{total_scripts:,}', bg=DARK_BG, fg=KPI_GREEN, bold=True, font_size=24, halign='CENTER',
                  note='Total .java test files across all repos'),
        make_cell('', bg=DARK_BG),
        make_cell(f'{total_modules}', bg=DARK_BG, fg=KPI_PURPLE, bold=True, font_size=24, halign='CENTER',
                  note='Distinct test module folders detected'),
        make_cell('', bg=DARK_BG),
        make_cell(f'{prod_v:,}', bg=DARK_BG, fg=KPI_AMBER, bold=True, font_size=24, halign='CENTER',
                  note='Verifications in production smoke/sanity suites'),
        make_cell('', bg=DARK_BG),
    ])
    # Label row
    rows.append([
        make_cell('Verifications', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('Scripts', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('Modules', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('Prod', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
    ])
    # Merges for KPI boxes: A-B, C-D, E-F, G-H, each spanning 2 rows
    for col_start in [0, 2, 4, 6]:
        merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': col_start, 'endColumnIndex': col_start + 2})
        merges.append({'startRowIndex': r + 1, 'endRowIndex': r + 2, 'startColumnIndex': col_start, 'endColumnIndex': col_start + 2})

    # Spacing before section
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))

    # ---- SECTION 2: SCRIPTS -> VERIFICATIONS ----
    print('    Section 2: Scripts to Verifications...')
    rows.append(_section_title_row('Test Scripts \u2192 Verifications', NC))

    # Header row for this section
    rows.append([
        header_cell('Platform'), header_cell('Scripts'), header_cell(''), header_cell(''),
        header_cell(''), header_cell(''), header_cell('Verif.'), header_cell('Ratio'),
    ])
    # Merge C-F for bar column header
    r = len(rows) - 1
    merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 2, 'endColumnIndex': 6})

    max_verif = max(platform_verifs.values()) if platform_verifs else 1
    bar_colors = {'Web': KPI_BLUE, 'Android': KPI_GREEN, 'iOS': KPI_PURPLE}
    for i, plat in enumerate(['Web', 'Android', 'iOS']):
        v = platform_verifs[plat]
        s = platform_scripts[plat]
        bar_len = int(v / max_verif * 30) if max_verif > 0 else 0
        bar_str = '\u2588' * bar_len
        multiplier = round(v / s, 1) if s > 0 else 0
        ratio_note = 'Verifications per script \u2014 higher means more efficient test coverage. Each script checks N things on average.' if i == 0 else None
        rows.append([
            make_cell(plat, bold=True, font_size=10, bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(s, font_size=10, halign='RIGHT', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(bar_str, fg=bar_colors[plat], font_size=10, bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(v, bold=True, font_size=10, halign='RIGHT', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(f'({multiplier}x)', font_size=10, halign='LEFT', bg=ALT_ROW_BG if i % 2 == 1 else None,
                      note=ratio_note),
        ])
        # Merge C-F for bar
        r = len(rows) - 1
        merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 2, 'endColumnIndex': 6})

    # Spacing before section
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))

    # ---- SECTION 3: EFFICIENCY & ROI METRICS ----
    print('    Section 3: Efficiency & ROI Metrics...')

    # Calculate ROI numbers from actual data
    roi_data = {}
    for plat, v_count in [('Web', web_v), ('Android', and_v), ('iOS', ios_v)]:
        manual_hrs = round(v_count * MANUAL_TIME_PER_VERIFICATION_MINS / 60, 1)
        auto_hrs = CI_TIMES_HOURS[plat]
        saved_hrs = round(manual_hrs - auto_hrs, 1)
        reduction = round(saved_hrs / manual_hrs * 100) if manual_hrs > 0 else 0
        roi_data[plat] = {'manual': manual_hrs, 'auto': auto_hrs, 'saved': saved_hrs, 'reduction': reduction}

    total_manual = round(sum(d['manual'] for d in roi_data.values()), 1)
    total_auto = sum(d['auto'] for d in roi_data.values())
    total_saved = round(total_manual - total_auto, 1)
    total_reduction = round(total_saved / total_manual * 100) if total_manual > 0 else 0

    rows.append(_section_title_row('Automation ROI', NC))

    # ROI KPI boxes - number row
    r = len(rows)
    rows.append([
        make_cell(f'~{int(total_manual)} hrs', bg=DARK_BG, fg=KPI_BLUE, bold=True, font_size=24, halign='CENTER',
                  note=f'Calculated as total_verifications x 2.5 min per verification. Industry standard for UI automation: 1-5 min per manual check, 2.5 min is conservative.'),
        make_cell('', bg=DARK_BG),
        make_cell(f'~{int(total_auto)} hrs', bg=DARK_BG, fg=KPI_GREEN, bold=True, font_size=24, halign='CENTER',
                  note=f'Based on CI regression run durations: Web ~4hrs, Android ~3hrs, iOS ~2hrs'),
        make_cell('', bg=DARK_BG),
        make_cell(f'~{int(total_saved)} hrs', bg=DARK_BG, fg=KPI_PURPLE, bold=True, font_size=24, halign='CENTER',
                  note='Manual effort minus automated time per regression cycle'),
        make_cell('', bg=DARK_BG),
        make_cell(f'{total_reduction}%', bg=DARK_BG, fg=KPI_AMBER, bold=True, font_size=24, halign='CENTER',
                  note='Percentage of manual effort eliminated by automation'),
        make_cell('', bg=DARK_BG),
    ])
    # Label row
    rows.append([
        make_cell('Manual Effort', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('Automated Time', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('Time Saved', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('Reduction', bg=DARK_BG, fg=LABEL_GRAY, font_size=10, halign='CENTER'),
        make_cell('', bg=DARK_BG),
    ])
    # Subtitle row for ROI boxes
    rows.append([
        make_cell('(all platforms)', bg=DARK_BG, fg=LABEL_GRAY, font_size=9, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('(CI parallel)', bg=DARK_BG, fg=LABEL_GRAY, font_size=9, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('per cycle', bg=DARK_BG, fg=LABEL_GRAY, font_size=9, halign='CENTER'),
        make_cell('', bg=DARK_BG),
        make_cell('', bg=DARK_BG),
        make_cell('', bg=DARK_BG),
    ])
    # Merges for ROI KPI boxes: 3 rows x 2 cols each
    for col_start in [0, 2, 4, 6]:
        for row_off in range(3):
            merges.append({'startRowIndex': r + row_off, 'endRowIndex': r + row_off + 1, 'startColumnIndex': col_start, 'endColumnIndex': col_start + 2})

    # Blank row
    rows.append(build_empty_row(NC))

    # Per-platform breakdown table
    rows.append([header_cell('Platform'), header_cell('Manual'), header_cell('Automated'),
                 header_cell('Saved'), header_cell('Reduction'), empty_cell(), empty_cell(), empty_cell()])
    for i, plat in enumerate(['Web', 'Android', 'iOS']):
        d = roi_data[plat]
        rows.append(build_data_row([
            plat, f'{d["manual"]} hrs', f'{d["auto"]} hrs', f'{d["saved"]} hrs', f'{d["reduction"]}%',
            '', '', ''
        ], alt=(i % 2 == 1)))
    rows.append(build_total_row([
        'Total', f'{total_manual} hrs', f'{total_auto} hrs', f'{total_saved} hrs', f'{total_reduction}%',
        '', '', ''
    ]))

    # Spacing before section
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))

    # ---- SECTION 4: MODULE COVERAGE HEATMAP ----
    print('    Section 4: Module Coverage Heatmap...')
    rows.append(_section_title_row('Module Coverage Map', NC,
                note='Coverage density per module per platform. \u25cf\u25cf\u25cf = 100+ verifications (high), \u25cf\u25cf = 21-99 (medium), \u25cf = 1-20 (low), \u2014 = no coverage (gap)'))
    # (no merge on section titles — prevents dark bar spanning all columns)

    # Header
    rows.append([header_cell('Module'), header_cell('Web'), header_cell('Android'), header_cell('iOS'),
                 empty_cell(), empty_cell(), empty_cell(), empty_cell()])

    # Collect all unique modules
    all_modules = sorted(set(list(web.keys()) + list(android.keys()) + list(ios.keys())))
    platform_data = {'Web': web, 'Android': android, 'iOS': ios}

    for idx, mod in enumerate(all_modules):
        row_cells = [make_cell(mod, bold=True, font_size=10, bg=ALT_ROW_BG if idx % 2 == 1 else None)]
        for plat in ['Web', 'Android', 'iOS']:
            pdata = platform_data[plat]
            if mod in pdata:
                count = len(pdata[mod]['assertions'])
            else:
                count = 0
            if count >= 100:
                row_cells.append(make_cell('\u25cf\u25cf\u25cf', bg=HEATMAP_HIGH_BG, bold=True, font_size=10, halign='CENTER'))
            elif count >= 21:
                row_cells.append(make_cell('\u25cf\u25cf', bg=HEATMAP_MED_BG, bold=True, font_size=10, halign='CENTER'))
            elif count >= 1:
                row_cells.append(make_cell('\u25cf', bg=HEATMAP_LOW_BG, bold=True, font_size=10, halign='CENTER'))
            else:
                row_cells.append(make_cell('\u2014', bg=HEATMAP_NONE_BG, fg=HEATMAP_NONE_FG, font_size=10, halign='CENTER'))
        row_cells += [empty_cell() for _ in range(4)]
        rows.append(row_cells)

    # Spacing before section
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))

    # ---- SECTION 5: PLATFORM MATURITY ----
    print('    Section 5: Platform Maturity...')
    rows.append(_section_title_row('Platform Maturity', NC,
                note='Maturity score based on: module coverage breadth (50%), verification depth per module (40%), and production suite presence (10%)'))
    # (no merge on section titles — prevents dark bar spanning all columns)

    prod_platforms = {'Web': False, 'Android': len(android_prod) > 0, 'iOS': len(ios_prod) > 0}
    for i, plat in enumerate(['Web', 'Android', 'iOS']):
        pdata = platform_data[plat]
        mod_count = len(pdata)
        module_coverage = mod_count / TOTAL_KNOWN_MODULES if TOTAL_KNOWN_MODULES > 0 else 0
        avg_verifs = (platform_verifs[plat] / mod_count) if mod_count > 0 else 0
        verification_depth = min(avg_verifs / 100, 1.0)
        prod_bonus = 0.1 if prod_platforms[plat] else 0
        maturity = min((module_coverage * 0.5 + verification_depth * 0.4 + prod_bonus) * 100, 100)
        maturity_int = int(round(maturity))

        filled = maturity_int // 4
        empty_blocks = 25 - filled
        bar_str = '\u2588' * filled + '\u2591' * empty_blocks

        if maturity_int >= 70:
            label = 'MATURE'
            label_fg = MATURE_GREEN
        elif maturity_int >= 40:
            label = 'GROWING'
            label_fg = GROWING_AMBER
        else:
            label = 'EARLY'
            label_fg = EARLY_PURPLE

        rows.append([
            make_cell(plat, bold=True, font_size=10, bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(bar_str, font_size=10, font_family='Courier New', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(f'{maturity_int}%', bold=True, font_size=10, halign='RIGHT', bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell(label, bold=True, font_size=10, fg=label_fg, bg=ALT_ROW_BG if i % 2 == 1 else None),
            make_cell('', bg=ALT_ROW_BG if i % 2 == 1 else None),
        ])
        # Merge B-E for bar
        r = len(rows) - 1
        merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 1, 'endColumnIndex': 5})

    # Spacing before section
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))
    rows.append(build_empty_row(NC))

    # ---- SECTION 6: ASSUMPTIONS & METHODOLOGY ----
    print('    Section 6: Assumptions & Methodology...')
    rows.append(_section_title_row('Assumptions & Methodology', NC))

    # Header
    rows.append([header_cell('Parameter'), header_cell('Value'), header_cell(''), header_cell(''),
                 header_cell('Source'), header_cell(''), header_cell(''), header_cell('')])
    # Merge Value cols (B-D) and Source cols (E-H)
    r = len(rows) - 1
    merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 1, 'endColumnIndex': 4})
    merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 4, 'endColumnIndex': 8})

    assumptions = [
        ('Manual verification time', '2.5 min per check', 'Industry standard (conservative). Range: 1-5 min depending on complexity'),
        ('Web CI regression time', '~4 hours', 'Based on Novo-P1-UI-Tests GitHub Actions run history'),
        ('Android CI regression time', '~3 hours', 'Estimated based on Appium mobile test execution benchmarks'),
        ('iOS CI regression time', '~2 hours', 'Estimated based on Appium mobile test execution benchmarks'),
        ('Verification types counted', 'isElementPresent, assertEquals, verifyElementText, assertTrue, assertFalse', 'Extracted via regex from Java test files + Page Object deep scan'),
        ('Excluded from count', 'Assert.fail (error handlers), waitForElement (pre-conditions)', 'These are not actual test verifications'),
        ('Module detection', 'Folder-based (e.g. tests/Web/Cards/ \u2192 Cards)', 'Auto-detected from repository structure'),
        ('Login deduplication', 'Login verifications counted once globally', 'Prevents inflation from login steps repeated in every test'),
    ]
    for i, (param, value, source) in enumerate(assumptions):
        bg = ALT_ROW_BG if i % 2 == 1 else None
        rows.append([
            make_cell(param, font_size=10, bold=True, bg=bg, wrap_strategy='WRAP'),
            make_cell(value, font_size=10, bg=bg, wrap_strategy='WRAP'),
            make_cell('', bg=bg), make_cell('', bg=bg),
            make_cell(source, font_size=10, bg=bg, wrap_strategy='WRAP'),
            make_cell('', bg=bg), make_cell('', bg=bg), make_cell('', bg=bg),
        ])
        r = len(rows) - 1
        merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 1, 'endColumnIndex': 4})
        merges.append({'startRowIndex': r, 'endRowIndex': r + 1, 'startColumnIndex': 4, 'endColumnIndex': 8})

    col_widths = [160, 100, 110, 120, 130, 100, 120, 120]
    return rows, col_widths, 4, merges  # freeze_rows=4


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
    seen = set()  # Deduplicate by (file, description)
    for mn in sorted(data.keys()):
        # Only show actual verifications — exclude pre-conditions
        for a in data[mn].get('assertions', []):
            if a.get('type') == 'Pre-condition':
                continue
            if a.get('type') == 'Assert.fail':
                continue
            # Deduplicate: skip if same file + same description already seen
            dedup_key = (a.get('file', ''), a['description'])
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
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


def hide_gridlines(spreadsheet, ws):
    """Hide gridlines on a sheet for cleaner look."""
    try:
        spreadsheet.batch_update({'requests': [{
            'updateSheetProperties': {
                'properties': {'sheetId': ws.id, 'gridProperties': {'hideGridlines': True}},
                'fields': 'gridProperties.hideGridlines'
            }
        }]})
    except Exception as e:
        print(f'  Gridlines warning: {e}')


def remove_charts(spreadsheet, ws):
    """Remove any existing chart objects from a sheet."""
    sid = ws.id
    try:
        meta = spreadsheet.fetch_sheet_metadata()
        for s in meta.get('sheets', []):
            if s['properties']['sheetId'] == sid:
                for c in s.get('charts', []):
                    spreadsheet.batch_update({'requests': [{'deleteEmbeddedObject': {'objectId': c['chartId']}}]})
    except Exception as e:
        print(f'  Chart removal warning: {e}')


def apply_merges(spreadsheet, ws, merges):
    """Apply cell merge requests to a sheet."""
    if not merges:
        return
    sid = ws.id
    requests = []
    for m in merges:
        requests.append({
            'mergeCells': {
                'range': {
                    'sheetId': sid,
                    'startRowIndex': m['startRowIndex'],
                    'endRowIndex': m['endRowIndex'],
                    'startColumnIndex': m['startColumnIndex'],
                    'endColumnIndex': m['endColumnIndex'],
                },
                'mergeType': 'MERGE_ALL'
            }
        })
    # Send in batches
    batch_size = 30
    for i in range(0, len(requests), batch_size):
        batch = requests[i:i + batch_size]
        spreadsheet.batch_update({'requests': batch})
        if i + batch_size < len(requests):
            time.sleep(0.5)


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
    print('  1/10 Grand Summary...')
    gs_rows, gs_widths, gs_freeze, gs_merges = build_grand_summary(web, android, ios, android_prod, ios_prod, ts)
    write_sheet_data(ss, grand_ws, gs_rows, gs_widths, gs_freeze)
    print(f'    {len(gs_rows)} rows written')
    print(f'    Applying {len(gs_merges)} cell merges...')
    apply_merges(ss, grand_ws, gs_merges)
    print('    Merges applied')
    time.sleep(1)

    # --- Web Summary ---
    print('  2/10 Web Summary...')
    ws = get_or_create(ss, 'Web Summary')
    r, cw, fr = build_platform_summary('Web Summary', web, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- Android Summary ---
    print('  3/10 Android Summary...')
    ws = get_or_create(ss, 'Android Summary')
    r, cw, fr = build_platform_summary('Android Summary', android, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- iOS Summary ---
    print('  4/10 iOS Summary...')
    ws = get_or_create(ss, 'iOS Summary')
    r, cw, fr = build_platform_summary('iOS Summary', ios, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- Production Suites ---
    print('  5/10 Production Suites...')
    ws = get_or_create(ss, 'Production Suites')
    r, cw, fr = build_prod_suites(android_prod, ios_prod, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # --- Detail sheets ---
    detail_configs = [
        ('Web - All', web, 300),
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
    print('  10/10 Verification Types...')
    ws = get_or_create(ss, 'Verification Types')
    r, cw, fr = build_verification_types(web, android, ios, android_prod, ios_prod, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # Step 4: Clean up Grand Summary
    print('\n[4/4] Cleaning up Grand Summary...')
    grand_ws = ss.worksheet('Grand Summary')
    remove_charts(ss, grand_ws)
    hide_gridlines(ss, grand_ws)
    print('  Removed old charts, hidden gridlines.')

    elapsed = time.time() - start_time
    print(f'\n{"=" * 60}')
    print(f'DONE! All 10 sheets rebuilt with executive dashboard in {elapsed:.1f}s')
    print(f'Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}')
    print(f'{"=" * 60}')


if __name__ == '__main__':
    main()
