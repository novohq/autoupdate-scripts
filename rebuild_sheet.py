#!/usr/bin/env python3
"""
NEXUS Sheet Rebuild - writes data + formatting in a single updateCells request per sheet.
Uses gspread for sheet management, raw batchUpdate API for cell data+format together.
"""

import os, sys, json, time, warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings('ignore')

# Add scripts dir to path so we can import nexus_scan
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from nexus_scan import (
    scan_repo, WEB_REPO, MOBILE_REPO, CREDS_PATH, SHEET_ID,
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
    'Assert.fail':       {'bg': rgb_dict(0.996, 0.886, 0.886), 'fg': rgb_dict(0.600, 0.106, 0.106)},
}

# ==================== CELL BUILDER ====================

def make_cell(value, bg=None, fg=None, bold=False, font_size=10, halign='LEFT',
              font_family='Inter', border_bottom=None, border_top=None):
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
    return cell


def title_cell(value, num_cols=1):
    """Title row cell."""
    return make_cell(value, bg=TITLE_BG, fg=TITLE_FG, bold=True, font_size=14)

def subtitle_cell(value):
    """Subtitle/timestamp cell."""
    return make_cell(value, fg=SUBTITLE_FG, font_size=10)

def header_cell(value):
    """Header row cell."""
    return make_cell(value, bg=HEADER_BG, fg=HEADER_FG, bold=True, font_size=10,
                     halign='CENTER', border_bottom=BLUE_BORDER)

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
                'fields': 'userEnteredValue,userEnteredFormat',
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

def build_grand_summary(web, android, ios, android_prod, ios_prod, ts):
    """Build all rows for Grand Summary sheet."""
    web_t = sum(len(m['assertions']) for m in web.values())
    and_t = sum(len(m['assertions']) for m in android.values())
    ios_t = sum(len(m['assertions']) for m in ios.values())
    andp_t = sum(len(m['assertions']) for m in android_prod.values())
    iosp_t = sum(len(m['assertions']) for m in ios_prod.values())
    grand = web_t + and_t + ios_t + andp_t + iosp_t

    NC = 6  # num cols
    rows = []
    rows.append(build_title_row('NEXUS Assertion Registry', NC))
    rows.append(build_subtitle_row(f'Last scanned: {ts}', NC))
    rows.append(build_empty_row(NC))
    rows.append(build_header_row(['Platform', 'Modules', 'Test Files', 'Assertions', 'Prod Suites', 'Total']))
    rows.append(build_data_row(['Web', len(web), sum(len(m['files']) for m in web.values()), web_t, 0, web_t], alt=False))
    rows.append(build_data_row(['Android', len(android), sum(len(m['files']) for m in android.values()), and_t, andp_t, and_t + andp_t], alt=True))
    rows.append(build_data_row(['iOS', len(ios), sum(len(m['files']) for m in ios.values()), ios_t, iosp_t, ios_t + iosp_t], alt=False))
    rows.append(build_total_row(['TOTAL', len(web)+len(android)+len(ios),
                                  sum(len(m['files']) for d in [web,android,ios] for m in d.values()),
                                  web_t+and_t+ios_t, andp_t+iosp_t, grand]))
    rows.append(build_empty_row(NC))

    # Assertion types table
    rows.append(build_header_row(['Assertion Types', 'Web', 'Android', 'iOS', 'Prod', 'Total']))
    all_types = sorted(set(t for d in [web,android,ios,android_prod,ios_prod] for m in d.values() for t in m['types']))
    for i, t in enumerate(all_types):
        wc = sum(m['types'].get(t,0) for m in web.values())
        ac = sum(m['types'].get(t,0) for m in android.values())
        ic = sum(m['types'].get(t,0) for m in ios.values())
        pc = sum(m['types'].get(t,0) for d in [android_prod,ios_prod] for m in d.values())
        rows.append(build_data_row([t, wc, ac, ic, pc, wc+ac+ic+pc], alt=(i % 2 == 1), type_col=0))
    rows.append(build_total_row(['TOTAL', web_t, and_t, ios_t, andp_t+iosp_t, grand]))

    rows.append(build_empty_row(NC))

    # Chart data - platform distribution
    rows.append(build_subtitle_row('Platform Distribution (chart data)', NC))
    rows.append(build_header_row(['Platform', 'Assertions', '', '', '', '']))
    chart_start = len(rows)  # 0-indexed row where chart data starts
    rows.append(build_data_row(['Web', web_t, '', '', '', ''], alt=False))
    rows.append(build_data_row(['Android', and_t, '', '', '', ''], alt=True))
    rows.append(build_data_row(['iOS', ios_t, '', '', '', ''], alt=False))
    rows.append(build_data_row(['Android Prod', andp_t, '', '', '', ''], alt=True))
    rows.append(build_data_row(['iOS Prod', iosp_t, '', '', '', ''], alt=False))

    rows.append(build_empty_row(NC))

    # Chart data - top modules
    rows.append(build_subtitle_row('Top 15 Modules (chart data)', NC))
    rows.append(build_header_row(['Module', 'Platform', 'Assertions', '', '', '']))
    mod_start = len(rows)
    all_mods = []
    for n, m in web.items(): all_mods.append((n, 'Web', len(m['assertions'])))
    for n, m in android.items(): all_mods.append((n, 'Android', len(m['assertions'])))
    for n, m in ios.items(): all_mods.append((n, 'iOS', len(m['assertions'])))
    all_mods.sort(key=lambda x: -x[2])
    for i, (mod, plat, cnt) in enumerate(all_mods[:15]):
        rows.append(build_data_row([mod, plat, cnt, '', '', ''], alt=(i % 2 == 1)))

    col_widths = [160, 100, 100, 120, 120, 120]
    return rows, col_widths, 4, chart_start, mod_start  # freeze_rows=4


def build_platform_summary(name, data, ts):
    """Build rows for a platform summary sheet."""
    headers = ['Module', 'Test Files', 'Assertions', 'isElementPresent', 'assertEquals',
               'verifyElementText', 'waitForElement', 'assertTrue/False', 'Assert.fail', 'Other']
    NC = len(headers)
    rows = []
    rows.append(build_title_row(name, NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    data_rows = []
    for mn in sorted(data.keys()):
        m = data[mn]; ty = m['types']
        data_rows.append([
            mn, len(m['files']), len(m['assertions']),
            ty.get('isElementPresent', 0), ty.get('assertEquals', 0),
            ty.get('verifyElementText', 0), ty.get('waitForElement', 0),
            ty.get('assertTrue', 0) + ty.get('assertFalse', 0),
            ty.get('Assert.fail', 0),
            sum(v for k, v in ty.items() if k not in ('isElementPresent', 'assertEquals', 'verifyElementText', 'waitForElement', 'assertTrue', 'assertFalse', 'Assert.fail'))
        ])
    for i, dr in enumerate(data_rows):
        rows.append(build_data_row(dr, alt=(i % 2 == 1)))

    # Totals
    totals = ['TOTAL', sum(len(m['files']) for m in data.values()), sum(len(m['assertions']) for m in data.values())]
    for c in range(3, 10):
        totals.append(sum(dr[c] for dr in data_rows if len(dr) > c))
    rows.append(build_total_row(totals))

    col_widths = [180, 90, 100, 130, 110, 130, 120, 120, 100, 80]
    return rows, col_widths, 3


def build_detail_sheet(sheet_name, data, ts, max_rows=None):
    """Build rows for a detail assertions sheet."""
    headers = ['Test File', 'Module', 'Number', 'Assertion', 'Type']
    NC = len(headers)
    rows = []
    rows.append(build_title_row(sheet_name, NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    num = 1
    for mn in sorted(data.keys()):
        for a in data[mn].get('assertions', []):
            rows.append(build_data_row(
                [a.get('file', ''), mn, num, a['description'], a['type']],
                alt=((num - 1) % 2 == 1),
                type_col=4
            ))
            num += 1
            if max_rows and num - 1 >= max_rows:
                break
        if max_rows and num - 1 >= max_rows:
            break

    col_widths = [250, 150, 80, 400, 140]
    return rows, col_widths, 3


def build_prod_suites(android_prod, ios_prod, ts):
    """Build rows for Production Suites sheet."""
    headers = ['Suite', 'Platform', 'Test Files', 'Assertions']
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


def build_assertion_types(web, android, ios, android_prod, ios_prod, ts):
    """Build rows for Assertion Types sheet."""
    headers = ['Type', 'Web', 'Android', 'iOS', 'Prod', 'Total']
    NC = len(headers)
    rows = []
    rows.append(build_title_row('Assertion Types Cross-Platform', NC))
    rows.append(build_subtitle_row(f'Scanned: {ts}', NC))
    rows.append(build_header_row(headers))

    web_t = sum(len(m['assertions']) for m in web.values())
    and_t = sum(len(m['assertions']) for m in android.values())
    ios_t = sum(len(m['assertions']) for m in ios.values())
    andp_t = sum(len(m['assertions']) for m in android_prod.values())
    iosp_t = sum(len(m['assertions']) for m in ios_prod.values())
    grand = web_t + and_t + ios_t + andp_t + iosp_t

    all_types = sorted(set(t for d in [web, android, ios, android_prod, ios_prod] for m in d.values() for t in m['types']))
    for i, t in enumerate(all_types):
        wc = sum(m['types'].get(t, 0) for m in web.values())
        ac = sum(m['types'].get(t, 0) for m in android.values())
        ic = sum(m['types'].get(t, 0) for m in ios.values())
        pc = sum(m['types'].get(t, 0) for d in [android_prod, ios_prod] for m in d.values())
        rows.append(build_data_row([t, wc, ac, ic, pc, wc+ac+ic+pc], alt=(i % 2 == 1), type_col=0))

    rows.append(build_total_row(['TOTAL', web_t, and_t, ios_t, andp_t + iosp_t, grand]))

    col_widths = [160, 100, 100, 100, 100, 100]
    return rows, col_widths, 3


# ==================== CHARTS ====================

def add_charts(spreadsheet, ws, chart_start, mod_start):
    """Add pie chart and bar chart to Grand Summary."""
    sid = ws.id
    try:
        # Delete existing charts using correct API field name
        meta = spreadsheet.fetch_sheet_metadata()
        existing_charts = []
        for s in meta.get('sheets', []):
            if s['properties']['sheetId'] == sid:
                existing_charts = [c['chartId'] for c in s.get('charts', [])]
        for cid in existing_charts:
            spreadsheet.batch_update({'requests': [{'deleteEmbeddedObject': {'objectId': cid}}]})

        requests = [
            # Pie chart
            {'addChart': {'chart': {
                'spec': {
                    'title': 'Assertions by Platform',
                    'pieChart': {
                        'legendPosition': 'RIGHT_LEGEND',
                        'domain': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': chart_start, 'endRowIndex': chart_start + 5, 'startColumnIndex': 0, 'endColumnIndex': 1}]}},
                        'series': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': chart_start, 'endRowIndex': chart_start + 5, 'startColumnIndex': 1, 'endColumnIndex': 2}]}},
                    }
                },
                'position': {'overlayPosition': {'anchorCell': {'sheetId': sid, 'rowIndex': 0, 'columnIndex': 7}, 'widthPixels': 420, 'heightPixels': 280}}
            }}},
            # Bar chart
            {'addChart': {'chart': {
                'spec': {
                    'title': 'Top 15 Modules by Assertions',
                    'basicChart': {
                        'chartType': 'BAR',
                        'legendPosition': 'NO_LEGEND',
                        'axis': [{'position': 'BOTTOM_AXIS', 'title': 'Assertions'}, {'position': 'LEFT_AXIS'}],
                        'domains': [{'domain': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': mod_start, 'endRowIndex': mod_start + 15, 'startColumnIndex': 0, 'endColumnIndex': 1}]}}}],
                        'series': [{'series': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': mod_start, 'endRowIndex': mod_start + 15, 'startColumnIndex': 2, 'endColumnIndex': 3}]}},
                                    'color': {'red': 0.23, 'green': 0.51, 'blue': 0.96}}],
                    }
                },
                'position': {'overlayPosition': {'anchorCell': {'sheetId': sid, 'rowIndex': 10, 'columnIndex': 7}, 'widthPixels': 520, 'heightPixels': 380}}
            }}}
        ]
        spreadsheet.batch_update({'requests': requests})
        print('  Charts added successfully')
    except Exception as e:
        print(f'  Charts warning: {e}')


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
    print('NEXUS Sheet Rebuild - Data + Formatting in One Request')
    print('=' * 60)
    print(f'Time: {ts}')

    # Step 1: Scan repos
    print('\n[1/4] Scanning repositories...')
    print('  Scanning Web repo...')
    web = scan_repo(WEB_REPO, 'src/test/java/tests', 'src/main/java/pages', 'web')
    for n in sorted(web):
        print(f'    {n}: {len(web[n]["files"])} files, {len(web[n]["assertions"])} assertions')

    print('  Scanning Android...')
    android = scan_repo(MOBILE_REPO, 'src/test/java/tests/android', 'src/main/java/pages/android', 'android')
    for n in sorted(android):
        print(f'    {n}: {len(android[n]["files"])} files, {len(android[n]["assertions"])} assertions')

    print('  Scanning iOS...')
    ios = scan_repo(MOBILE_REPO, 'src/test/java/tests/iOS', 'src/main/java/pages/iOS', 'ios')
    for n in sorted(ios):
        print(f'    {n}: {len(ios[n]["files"])} files, {len(ios[n]["assertions"])} assertions')

    print('  Scanning Prod suites...')
    android_prod = scan_repo(MOBILE_REPO, 'src/test/java/tests/androidProdSanitySuite', 'src/main/java/pages/android', 'android')
    ios_prod = scan_repo(MOBILE_REPO, 'src/test/java/tests/iOSProdSuite', 'src/main/java/pages/iOS', 'ios')
    print(f'    Android Prod: {sum(len(m["assertions"]) for m in android_prod.values())}')
    print(f'    iOS Prod: {sum(len(m["assertions"]) for m in ios_prod.values())}')

    total = sum(len(m['assertions']) for d in [web, android, ios, android_prod, ios_prod] for m in d.values())
    print(f'\n  GRAND TOTAL: {total} assertions')

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
    gs_rows, gs_widths, gs_freeze, chart_start, mod_start = build_grand_summary(web, android, ios, android_prod, ios_prod, ts)
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

    # --- Assertion Types ---
    print('  12/12 Assertion Types...')
    ws = get_or_create(ss, 'Assertion Types')
    r, cw, fr = build_assertion_types(web, android, ios, android_prod, ios_prod, ts)
    write_sheet_data(ss, ws, r, cw, fr)
    print(f'    {len(r)} rows written')
    time.sleep(0.5)

    # Step 4: Charts
    print('\n[4/4] Adding charts to Grand Summary...')
    grand_ws = ss.worksheet('Grand Summary')
    add_charts(ss, grand_ws, chart_start, mod_start)

    elapsed = time.time() - start_time
    print(f'\n{"=" * 60}')
    print(f'DONE! All 12 sheets rebuilt with data + formatting in {elapsed:.1f}s')
    print(f'Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}')
    print(f'{"=" * 60}')


if __name__ == '__main__':
    main()
