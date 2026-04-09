#!/usr/bin/env python3
"""
NEXUS Assertion Scanner v2
Scans Java test repos for assertions, updates Google Sheets with formatting.
Usage: python3 nexus_scan.py [--web-only] [--mobile-only] [--dry-run]
"""

import os, re, sys, json, warnings
from pathlib import Path
from datetime import datetime

warnings.filterwarnings('ignore')

# ==================== CONFIG ====================
WEB_REPO = os.environ.get('WEB_REPO_PATH', '/Users/kawal/Research/QE/Novo-P1-UI-Tests')
MOBILE_REPO = os.environ.get('MOBILE_REPO_PATH', '/Users/kawal/Research/QE/Novo_Mobile_UIAutomation_Appium')
CREDS_PATH = os.environ.get('GOOGLE_SHEETS_CREDS', '/Users/kawal/Research/QE/.secrets/google-sheets-creds.json')
SHEET_ID = os.environ.get('NEXUS_SHEET_ID', '1QqQzDIfZ4RWRfqYOmy2YnfJCF1inGn7rhp2a6CiEdv8')

ASSERTION_PATTERNS = [
    (r'Assert\.assertEquals\s*\(', 'assertEquals'),
    (r'Assert\.assertTrue\s*\(', 'assertTrue'),
    (r'Assert\.assertFalse\s*\(', 'assertFalse'),
    (r'Assert\.fail\s*\(', 'Assert.fail'),
    (r'isElementPresent\s*\(', 'isElementPresent'),
    (r'isElementDisplayed\s*\(', 'isElementPresent'),
    (r'verifyElementText\s*\(', 'verifyElementText'),
    (r'waitForElement\s*\(', 'waitForElement'),
    (r'waitForVisibility\s*\(', 'waitForElement'),
    (r'\.isDisplayed\s*\(\s*\)', 'isElementPresent'),
    (r'\.isEnabled\s*\(\s*\)', 'isElementPresent'),
    (r'\.getText\s*\(\s*\)\.equals\s*\(', 'verifyElementText'),
    (r'\.getText\s*\(\s*\)\.contains\s*\(', 'verifyElementText'),
]

# Color map for formatting
TYPE_COLORS = {
    'assertEquals':      {'bg': (0.82, 0.95, 0.87), 'fg': (0.02, 0.37, 0.27)},  # green
    'assertTrue':        {'bg': (0.82, 0.95, 0.87), 'fg': (0.02, 0.37, 0.27)},
    'assertFalse':       {'bg': (0.82, 0.95, 0.87), 'fg': (0.02, 0.37, 0.27)},
    'isElementPresent':  {'bg': (0.86, 0.92, 0.98), 'fg': (0.12, 0.25, 0.50)},  # blue
    'verifyElementText': {'bg': (0.81, 0.98, 0.98), 'fg': (0.08, 0.37, 0.46)},  # cyan
    'waitForElement':    {'bg': (1.00, 0.95, 0.78), 'fg': (0.57, 0.25, 0.05)},  # amber
    'Assert.fail':       {'bg': (0.99, 0.87, 0.87), 'fg': (0.60, 0.10, 0.10)},  # red
}
HEADER_COLOR = {'bg': (0.06, 0.09, 0.16), 'fg': (1.0, 1.0, 1.0)}  # dark navy + white
TOTAL_COLOR = {'bg': (0.12, 0.16, 0.24), 'fg': (0.23, 0.51, 0.96)}  # slate + blue

# ==================== SCANNER ====================

def extract_assertions_from_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.read().split('\n')
    except:
        return []

    assertions = []
    current_method = 'Unknown'
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('//') or stripped.startswith('*') or stripped.startswith('/*'):
            continue
        # Track current method name
        m = re.search(r'(?:public|private|protected)\s+\w+\s+(\w+)\s*\(', stripped)
        if m:
            current_method = m.group(1)
        for pattern, atype in ASSERTION_PATTERNS:
            if re.search(pattern, line):
                desc = build_description(line, lines, i, atype, current_method)
                assertions.append({'line': i+1, 'type': atype, 'description': desc, 'method': current_method})
                break
    return assertions


def build_description(line, lines, idx, atype, method):
    stripped = line.strip()
    # Check ExtentReport log above
    for lb in range(1, 6):
        if idx - lb >= 0:
            prev = lines[idx - lb].strip()
            m = re.search(r'(?:test\.log|extentTest\.log)\s*\(.*?"(.+?)"', prev)
            if m: return m.group(1)[:100]
            m = re.search(r'log\s*\(.*?Status\.\w+\s*,\s*"(.+?)"', prev)
            if m: return m.group(1)[:100]

    if atype == 'assertEquals':
        m = re.search(r'assertEquals\s*\(\s*(.+?)\s*,\s*(.+?)\s*[,)]', stripped)
        if m: return f'Verify {clean(m.group(1))} == {clean(m.group(2))}'
    elif atype in ('isElementPresent', 'waitForElement'):
        m = re.search(r'(?:isElementPresent|waitForElement|waitForVisibility)\s*\(\s*(\w+)', stripped)
        if m: return f'Verify {camel_to_words(m.group(1))} is visible'
        m = re.search(r'\.isDisplayed\s*\(\s*\)', stripped)
        if m:
            var = re.search(r'(\w+)\.isDisplayed', stripped)
            if var: return f'Verify {camel_to_words(var.group(1))} is displayed'
    elif atype == 'verifyElementText':
        m = re.search(r'verifyElementText\s*\(\s*(\w+)\s*,\s*"(.+?)"', stripped)
        if m: return f'Verify {camel_to_words(m.group(1))} text == "{m.group(2)[:50]}"'
        m = re.search(r'getText\s*\(\s*\)\.(?:equals|contains)\s*\(\s*"(.+?)"', stripped)
        if m: return f'Verify text contains "{m.group(1)[:50]}"'
    elif atype in ('assertTrue', 'assertFalse'):
        m = re.search(r'assert(?:True|False)\s*\(\s*(.+?)\s*[,)]', stripped)
        if m: return f'Verify {clean(m.group(1))}'
    elif atype == 'Assert.fail':
        m = re.search(r'Assert\.fail\s*\(\s*"?(.+?)"?\s*\)', stripped)
        if m and m.group(1).strip(): return f'Fail: {clean(m.group(1))}'
        return f'{camel_to_words(method)} → catch block Assert.fail'

    return f'{camel_to_words(method)} → {atype}'


def clean(s):
    return s.strip().strip('"').strip("'")[:60]

def camel_to_words(s):
    return re.sub(r'([A-Z])', r' \1', s).strip().lower()


def detect_module(filepath, platform):
    parts = Path(filepath).parts
    if platform == 'web':
        for i, p in enumerate(parts):
            if p == 'Web' and i+1 < len(parts) and parts[i+1] != Path(filepath).name:
                return parts[i+1]
            if p == 'Onboarding' and (i+1 >= len(parts) or parts[i+1] == Path(filepath).name):
                return 'Onboarding'
            if p == 'Onboarding' and i+1 < len(parts) and parts[i+1] != Path(filepath).name:
                return parts[i+1]
    elif platform in ('android', 'ios'):
        for i, p in enumerate(parts):
            if p in ('android', 'iOS', 'androidProdSanitySuite', 'iOSProdSuite'):
                if i+1 < len(parts) and parts[i+1] != Path(filepath).name:
                    return parts[i+1]
    return Path(filepath).stem


def deep_scan_pages(test_file, page_dir):
    try:
        with open(test_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except:
        return []

    calls = set(re.findall(r'(\w+(?:[Pp]age)\w*)\.(\w+)\s*\(', content))
    results = []
    page_files = {pf.stem: pf for pf in Path(page_dir).rglob('*.java')} if os.path.exists(page_dir) else {}

    for page_var, method_name in calls:
        for pf_name, pf_path in page_files.items():
            try:
                with open(str(pf_path), 'r', encoding='utf-8', errors='ignore') as f:
                    pc = f.read()
            except:
                continue
            mp = rf'(?:public|private|protected)\s+\w+\s+{re.escape(method_name)}\s*\('
            mm = re.search(mp, pc)
            if not mm: continue
            # Extract method body
            start = mm.start()
            depth = 0; body = ''; active = False
            for ch in pc[start:start+5000]:
                if ch == '{': depth += 1; active = True
                elif ch == '}': depth -= 1
                if active: body += ch
                if active and depth == 0: break
            for i, line in enumerate(body.split('\n')):
                if line.strip().startswith('//') or line.strip().startswith('*'): continue
                for pattern, atype in ASSERTION_PATTERNS:
                    if re.search(pattern, line):
                        desc = build_description(line, body.split('\n'), i, atype, method_name)
                        results.append({'line': 0, 'type': atype, 'description': f'[{method_name}] {desc}', 'method': method_name})
                        break
            break
    return results


def scan_repo(repo_path, test_dir, page_dir, platform):
    test_path = os.path.join(repo_path, test_dir)
    page_path = os.path.join(repo_path, page_dir) if page_dir else None
    if not os.path.exists(test_path): return {}

    modules = {}
    for tf in sorted(Path(test_path).rglob('*.java')):
        if tf.name in ('BaseTest.java', 'baseTest.java', 'OnboardingBaseTest.java'): continue
        if 'Runner' in tf.name: continue

        module = detect_module(str(tf), platform)
        test_asserts = extract_assertions_from_file(str(tf))
        page_asserts = deep_scan_pages(str(tf), page_path) if page_path else []
        all_asserts = test_asserts + page_asserts

        if module not in modules:
            modules[module] = {'files': set(), 'assertions': [], 'types': {}}
        modules[module]['files'].add(tf.name)
        for a in all_asserts:
            a['file'] = tf.name
            a['module'] = module
            modules[module]['assertions'].append(a)
            modules[module]['types'][a['type']] = modules[module]['types'].get(a['type'], 0) + 1

    return modules


# ==================== GOOGLE SHEETS WITH FORMATTING ====================

def get_client():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if os.path.exists(CREDS_PATH):
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_PATH, scope)
    else:
        creds_json = json.loads(os.environ.get('GOOGLE_SHEETS_CREDS_JSON', '{}'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    return gspread.authorize(creds)


def rgb(r, g, b):
    return {'red': r, 'green': g, 'blue': b}


def cell_format(bg=None, fg=None, bold=False, font_size=10, halign='LEFT', font_family='Arial'):
    fmt = {
        'textFormat': {'bold': bold, 'fontSize': font_size, 'fontFamily': font_family},
        'horizontalAlignment': halign,
    }
    if fg: fmt['textFormat']['foregroundColorStyle'] = {'rgbColor': rgb(*fg)}
    if bg: fmt['backgroundColorStyle'] = {'rgbColor': rgb(*bg)}
    return fmt


def format_sheet(spreadsheet, ws, header_row=1, data_start=2, data_end=None, num_cols=6, col_widths=None, freeze_rows=0, type_col=None):
    """Apply professional formatting to a worksheet."""
    sheet_id = ws.id
    requests = []

    # Freeze header rows
    if freeze_rows:
        requests.append({'updateSheetProperties': {
            'properties': {'sheetId': sheet_id, 'gridProperties': {'frozenRowCount': freeze_rows}},
            'fields': 'gridProperties.frozenRowCount'
        }})

    # Column widths
    if col_widths:
        for i, w in enumerate(col_widths):
            requests.append({'updateDimensionProperties': {
                'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i+1},
                'properties': {'pixelSize': w}, 'fields': 'pixelSize'
            }})

    # Header row formatting
    requests.append({'repeatCell': {
        'range': {'sheetId': sheet_id, 'startRowIndex': header_row-1, 'endRowIndex': header_row, 'startColumnIndex': 0, 'endColumnIndex': num_cols},
        'cell': {'userEnteredFormat': cell_format(bg=HEADER_COLOR['bg'], fg=HEADER_COLOR['fg'], bold=True, font_size=10)},
        'fields': 'userEnteredFormat'
    }})

    # Data rows - alternate row shading
    if data_end:
        for r in range(data_start-1, data_end):
            if (r - data_start + 1) % 2 == 0:
                requests.append({'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 0, 'endColumnIndex': num_cols},
                    'cell': {'userEnteredFormat': {'backgroundColorStyle': {'rgbColor': rgb(0.96, 0.97, 0.98)}}},
                    'fields': 'userEnteredFormat.backgroundColorStyle'
                }})

    # Type column color coding
    if type_col is not None and data_end:
        values = ws.col_values(type_col + 1)
        for r in range(data_start-1, min(data_end, len(values))):
            val = values[r] if r < len(values) else ''
            colors = TYPE_COLORS.get(val, TYPE_COLORS.get('Assert.fail', None))
            if colors:
                requests.append({'repeatCell': {
                    'range': {'sheetId': sheet_id, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': type_col, 'endColumnIndex': type_col+1},
                    'cell': {'userEnteredFormat': cell_format(bg=colors['bg'], fg=colors['fg'], bold=True, font_size=9)},
                    'fields': 'userEnteredFormat'
                }})

    # Borders on header
    requests.append({'updateBorders': {
        'range': {'sheetId': sheet_id, 'startRowIndex': header_row-1, 'endRowIndex': header_row, 'startColumnIndex': 0, 'endColumnIndex': num_cols},
        'bottom': {'style': 'SOLID', 'width': 2, 'colorStyle': {'rgbColor': rgb(0.23, 0.51, 0.96)}}
    }})

    if requests:
        spreadsheet.batch_update({'requests': requests})


def update_sheet(web, android, ios, android_prod, ios_prod, dry_run=False):
    client = get_client()
    ss = client.open_by_key(SHEET_ID)
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    web_t = sum(len(m['assertions']) for m in web.values())
    and_t = sum(len(m['assertions']) for m in android.values())
    ios_t = sum(len(m['assertions']) for m in ios.values())
    andp_t = sum(len(m['assertions']) for m in android_prod.values())
    iosp_t = sum(len(m['assertions']) for m in ios_prod.values())
    grand = web_t + and_t + ios_t + andp_t + iosp_t

    # ==================== GRAND SUMMARY ====================
    ws = get_or_create(ss, 'Grand Summary')
    rows = [
        ['NEXUS Assertion Registry', '', '', '', '', ''],
        [f'Last scanned: {ts}', '', '', '', '', ''],
        [''],
        ['Platform', 'Modules', 'Test Files', 'Assertions', 'Prod Suites', 'Total'],
        ['Web', len(web), sum(len(m['files']) for m in web.values()), web_t, 0, web_t],
        ['Android', len(android), sum(len(m['files']) for m in android.values()), and_t, andp_t, and_t + andp_t],
        ['iOS', len(ios), sum(len(m['files']) for m in ios.values()), ios_t, iosp_t, ios_t + iosp_t],
        ['TOTAL', len(web)+len(android)+len(ios),
         sum(len(m['files']) for d in [web,android,ios] for m in d.values()),
         web_t+and_t+ios_t, andp_t+iosp_t, grand],
        [''],
        ['Assertion Types', 'Web', 'Android', 'iOS', 'Prod', 'Total'],
    ]
    all_types = sorted(set(t for d in [web,android,ios,android_prod,ios_prod] for m in d.values() for t in m['types']))
    for t in all_types:
        wc = sum(m['types'].get(t,0) for m in web.values())
        ac = sum(m['types'].get(t,0) for m in android.values())
        ic = sum(m['types'].get(t,0) for m in ios.values())
        pc = sum(m['types'].get(t,0) for d in [android_prod,ios_prod] for m in d.values())
        rows.append([t, wc, ac, ic, pc, wc+ac+ic+pc])
    type_end = len(rows)
    rows.append(['TOTAL', web_t, and_t, ios_t, andp_t+iosp_t, grand])

    rows += [[''], ['Platform Distribution (chart data)'], ['Platform', 'Assertions']]
    chart_start = len(rows)
    rows.append(['Web', web_t])
    rows.append(['Android', and_t])
    rows.append(['iOS', ios_t])
    rows.append(['Android Prod', andp_t])
    rows.append(['iOS Prod', iosp_t])

    rows += [[''], ['Top 15 Modules (chart data)'], ['Module', 'Platform', 'Assertions']]
    mod_start = len(rows)
    all_mods = []
    for n, m in web.items(): all_mods.append((n, 'Web', len(m['assertions'])))
    for n, m in android.items(): all_mods.append((n, 'Android', len(m['assertions'])))
    for n, m in ios.items(): all_mods.append((n, 'iOS', len(m['assertions'])))
    all_mods.sort(key=lambda x: -x[2])
    for mod, plat, cnt in all_mods[:15]:
        rows.append([mod, plat, cnt])

    if not dry_run:
        ws.clear()
        ws.update(range_name='A1', values=rows)
        format_sheet(ss, ws, header_row=4, data_start=5, data_end=8, num_cols=6,
                     col_widths=[160, 100, 100, 120, 120, 120], freeze_rows=4)
        # Format type table header
        format_sheet(ss, ws, header_row=10, data_start=11, data_end=type_end+1, num_cols=6, type_col=0)
        # Add charts
        add_charts(ss, ws, chart_start, mod_start)
        print(f'  Grand Summary: {len(rows)} rows + charts + formatting')

    # ==================== PLATFORM SUMMARIES ====================
    for name, data in [('Web Summary', web), ('Android Summary', android), ('iOS Summary', ios)]:
        ws = get_or_create(ss, name)
        hdrs = ['Module', 'Test Files', 'Assertions', 'isElementPresent', 'assertEquals', 'verifyElementText', 'waitForElement', 'assertTrue/False', 'Assert.fail', 'Other']
        r = [[f'{name}'], [f'Scanned: {ts}'], hdrs]
        for mn in sorted(data.keys()):
            m = data[mn]; ty = m['types']
            r.append([mn, len(m['files']), len(m['assertions']),
                       ty.get('isElementPresent',0), ty.get('assertEquals',0),
                       ty.get('verifyElementText',0), ty.get('waitForElement',0),
                       ty.get('assertTrue',0)+ty.get('assertFalse',0),
                       ty.get('Assert.fail',0),
                       sum(v for k,v in ty.items() if k not in ('isElementPresent','assertEquals','verifyElementText','waitForElement','assertTrue','assertFalse','Assert.fail'))])
        totals = ['TOTAL', sum(len(m['files']) for m in data.values()), sum(len(m['assertions']) for m in data.values())]
        for c in range(3, 10): totals.append(sum(row[c] for row in r[3:] if len(row) > c and isinstance(row[c], (int, float))))
        r.append(totals)

        if not dry_run:
            ws.clear()
            ws.update(range_name='A1', values=r)
            format_sheet(ss, ws, header_row=3, data_start=4, data_end=len(r), num_cols=10,
                         col_widths=[180, 90, 100, 130, 110, 130, 120, 120, 100, 80], freeze_rows=3)
            print(f'  {name}: {len(r)} rows + formatting')

    # ==================== DETAIL SHEETS ====================
    details = [
        ('Web - Cards', {'Cards': web.get('Cards', {'assertions':[], 'files': set(), 'types': {}})}),
        ('Web - Checking', {'Checking': web.get('Checking', {'assertions':[], 'files': set(), 'types': {}})}),
        ('Web - Invoices', {'Invoices': web.get('Invoices', {'assertions':[], 'files': set(), 'types': {}})}),
        ('Android - All', android),
        ('iOS - All', ios),
        ('Production - All', {**android_prod, **ios_prod}),
    ]
    for sheet_name, data in details:
        ws = get_or_create(ss, sheet_name)
        r = [[sheet_name], [f'Scanned: {ts}'], ['Test File', 'Module', 'Number', 'Assertion', 'Type']]
        num = 1
        for mn in sorted(data.keys()):
            for a in data[mn].get('assertions', []):
                r.append([a.get('file',''), mn, num, a['description'], a['type']])
                num += 1
        if not dry_run:
            ws.clear()
            ws.update(range_name='A1', values=r)
            format_sheet(ss, ws, header_row=3, data_start=4, data_end=len(r), num_cols=5,
                         col_widths=[250, 150, 80, 400, 140], freeze_rows=3, type_col=4)
            print(f'  {sheet_name}: {len(r)} rows + formatting')

    # ==================== PROD SUITES + ASSERTION TYPES ====================
    ws = get_or_create(ss, 'Production Suites')
    r = [['Production Suites'], [f'Scanned: {ts}'], ['Suite', 'Platform', 'Test Files', 'Assertions']]
    for mn, m in android_prod.items(): r.append([mn, 'Android', len(m['files']), len(m['assertions'])])
    for mn, m in ios_prod.items(): r.append([mn, 'iOS', len(m['files']), len(m['assertions'])])
    r.append(['TOTAL', '', sum(len(m['files']) for d in [android_prod,ios_prod] for m in d.values()), andp_t+iosp_t])
    if not dry_run:
        ws.clear(); ws.update(range_name='A1', values=r)
        format_sheet(ss, ws, header_row=3, data_start=4, data_end=len(r), num_cols=4,
                     col_widths=[250, 120, 100, 120], freeze_rows=3)
        print(f'  Production Suites: {len(r)} rows')

    ws = get_or_create(ss, 'Assertion Types')
    r = [['Assertion Types Cross-Platform'], [f'Scanned: {ts}'], ['Type', 'Web', 'Android', 'iOS', 'Prod', 'Total']]
    for t in all_types:
        wc = sum(m['types'].get(t,0) for m in web.values())
        ac = sum(m['types'].get(t,0) for m in android.values())
        ic = sum(m['types'].get(t,0) for m in ios.values())
        pc = sum(m['types'].get(t,0) for d in [android_prod,ios_prod] for m in d.values())
        r.append([t, wc, ac, ic, pc, wc+ac+ic+pc])
    r.append(['TOTAL', web_t, and_t, ios_t, andp_t+iosp_t, grand])
    if not dry_run:
        ws.clear(); ws.update(range_name='A1', values=r)
        format_sheet(ss, ws, header_row=3, data_start=4, data_end=len(r), num_cols=6,
                     col_widths=[160, 100, 100, 100, 100, 100], freeze_rows=3, type_col=0)
        print(f'  Assertion Types: {len(r)} rows')

    return grand


def get_or_create(ss, title):
    try: return ss.worksheet(title)
    except: return ss.add_worksheet(title=title, rows=2000, cols=20)


def add_charts(ss, ws, chart_start, mod_start):
    try:
        sid = ws.id
        # Delete existing charts first
        meta = ss.fetch_sheet_metadata()
        existing_charts = []
        for s in meta.get('sheets', []):
            if s['properties']['sheetId'] == sid:
                existing_charts = [c['chartId'] for c in s.get('charts', [])]
        for cid in existing_charts:
            ss.batch_update({'requests': [{'deleteEmbeddedChart': {'chartId': cid}}]})

        requests = [
            # Pie chart - Platform distribution
            {'addChart': {'chart': {
                'spec': {'title': 'Assertions by Platform', 'pieChart': {
                    'legendPosition': 'RIGHT_LEGEND',
                    'domain': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': chart_start, 'endRowIndex': chart_start+5, 'startColumnIndex': 0, 'endColumnIndex': 1}]}},
                    'series': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': chart_start, 'endRowIndex': chart_start+5, 'startColumnIndex': 1, 'endColumnIndex': 2}]}},
                }},
                'position': {'overlayPosition': {'anchorCell': {'sheetId': sid, 'rowIndex': 0, 'columnIndex': 7}, 'widthPixels': 420, 'heightPixels': 280}}
            }}},
            # Bar chart - Top modules
            {'addChart': {'chart': {
                'spec': {'title': 'Top 15 Modules by Assertions', 'basicChart': {
                    'chartType': 'BAR', 'legendPosition': 'NO_LEGEND',
                    'axis': [{'position': 'BOTTOM_AXIS', 'title': 'Assertions'}, {'position': 'LEFT_AXIS'}],
                    'domains': [{'domain': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': mod_start, 'endRowIndex': mod_start+15, 'startColumnIndex': 0, 'endColumnIndex': 1}]}}}],
                    'series': [{'series': {'sourceRange': {'sources': [{'sheetId': sid, 'startRowIndex': mod_start, 'endRowIndex': mod_start+15, 'startColumnIndex': 2, 'endColumnIndex': 3}]}},
                                'color': {'red': 0.23, 'green': 0.51, 'blue': 0.96}}],
                }},
                'position': {'overlayPosition': {'anchorCell': {'sheetId': sid, 'rowIndex': 10, 'columnIndex': 7}, 'widthPixels': 520, 'heightPixels': 380}}
            }}}
        ]
        ss.batch_update({'requests': requests})
    except Exception as e:
        print(f'  Charts warning: {e}')


# ==================== MAIN ====================

def main():
    args = sys.argv[1:]
    dry_run = '--dry-run' in args

    print(f'NEXUS Assertion Scanner v2')
    print(f'{"="*50}')
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    if dry_run: print('MODE: DRY RUN')

    web, android, ios, aprod, iprod = {}, {}, {}, {}, {}

    if '--mobile-only' not in args:
        print('\nScanning Web repo...')
        web = scan_repo(WEB_REPO, 'src/test/java/tests', 'src/main/java/pages', 'web')
        for n in sorted(web): print(f'  {n}: {len(web[n]["files"])} files, {len(web[n]["assertions"])} assertions')

    if '--web-only' not in args:
        print('\nScanning Android...')
        android = scan_repo(MOBILE_REPO, 'src/test/java/tests/android', 'src/main/java/pages/android', 'android')
        for n in sorted(android): print(f'  {n}: {len(android[n]["files"])} files, {len(android[n]["assertions"])} assertions')

        print('\nScanning iOS...')
        ios = scan_repo(MOBILE_REPO, 'src/test/java/tests/iOS', 'src/main/java/pages/iOS', 'ios')
        for n in sorted(ios): print(f'  {n}: {len(ios[n]["files"])} files, {len(ios[n]["assertions"])} assertions')

        print('\nScanning Prod suites...')
        aprod = scan_repo(MOBILE_REPO, 'src/test/java/tests/androidProdSanitySuite', 'src/main/java/pages/android', 'android')
        iprod = scan_repo(MOBILE_REPO, 'src/test/java/tests/iOSProdSuite', 'src/main/java/pages/iOS', 'ios')
        print(f'  Android Prod: {sum(len(m["assertions"]) for m in aprod.values())}')
        print(f'  iOS Prod: {sum(len(m["assertions"]) for m in iprod.values())}')

    total = sum(len(m['assertions']) for d in [web,android,ios,aprod,iprod] for m in d.values())
    print(f'\n{"="*50}\nGRAND TOTAL: {total}\n{"="*50}')

    print('\nUpdating Google Sheet...')
    update_sheet(web, android, ios, aprod, iprod, dry_run)
    print(f'\nDone! {datetime.now().strftime("%H:%M:%S")}')


if __name__ == '__main__':
    main()
