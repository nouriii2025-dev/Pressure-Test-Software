import csv
import os
import struct
import threading
import time
from datetime import datetime, timedelta
import io
import json
import math
from flask import Flask, jsonify, render_template, request, send_file
from pymodbus.client import ModbusTcpClient
import ipaddress
import re
import base64

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(BASE_DIR, 'settings.json')

DEFAULT_SETTINGS = {
    'ip_address': '',
    'port': 502,
    'device_id': 255,
}
TOTAL_CHANNELS = 6

ARCHIVE_LOG_FILE = 'abb_data_log.csv'
LOG_DIR = 'test_logs'
REPORT_DIR = 'reports'

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

LOGO_FILE = os.path.join(BASE_DIR, 'weatherford_logo.png')

def get_logo_data_uri():
    """Return the logo as a data URI so it survives 'Save Report'."""
    try:
        with open(LOGO_FILE, 'rb') as f:
            return 'data:image/png;base64,' + base64.b64encode(f.read()).decode('ascii')
    except Exception as exc:
        print(f"[LOGO WARN] {exc}")
        return ''


app.config['JSON_SORT_KEYS'] = False

state_lock = threading.RLock()

state = {
    'running': False,
    'session': None,
    'error': None,
    'thread': None,
}

# ---------------------------------------------------------------------------
# Instrument connection settings (persisted in settings.json)
# ---------------------------------------------------------------------------
settings_lock = threading.Lock()
settings = dict(DEFAULT_SETTINGS)

_HOSTNAME_RE = re.compile(
    r'^(?=.{1,253}$)([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$'
)


def validate_settings(data):
    """Return (clean_settings, error_message)."""
    data = data or {}
    ip = str(data.get('ip_address', '')).strip()
    if not ip:
        return None, 'IP address is required.'

    try:
        ipaddress.ip_address(ip)
    except ValueError:
        if not _HOSTNAME_RE.match(ip):
            return None, 'Enter a valid IP address (e.g. 192.168.1.135).'

    try:
        port = int(data.get('port', DEFAULT_SETTINGS['port']))
        device_id = int(data.get('device_id', DEFAULT_SETTINGS['device_id']))
    except (TypeError, ValueError):
        return None, 'Port and Device ID must be numbers.'

    if not 1 <= port <= 65535:
        return None, 'Port must be between 1 and 65535.'
    if not 0 <= device_id <= 255:
        return None, 'Device ID must be between 0 and 255.'

    return {'ip_address': ip, 'port': port, 'device_id': device_id}, None


def load_settings():
    global settings
    loaded = dict(DEFAULT_SETTINGS)
    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            clean, err = validate_settings({**DEFAULT_SETTINGS, **json.load(f)})
            if not err:
                loaded = clean
            else:
                print(f"[SETTINGS WARN] {err} Using defaults.")
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"[SETTINGS WARN] Could not read settings: {exc}")
    with settings_lock:
        settings = loaded


def save_settings_to_disk(clean):
    tmp = SETTINGS_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(clean, f, indent=2)
    os.replace(tmp, SETTINGS_FILE)  # atomic, avoids a half-written file


def get_settings():
    with settings_lock:
        return dict(settings)


load_settings()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def format_elapsed(seconds):
    return str(timedelta(seconds=int(seconds or 0)))


def _fmt(v):
    return f"{float(v):.2f}" if v is not None else '--'


def test_history(session, ch):
    """Samples from the Hold point to the Stop point."""
    hist = session['channel_data'][ch]['history']
    if not session.get('uses_hold'):      # imported CSV sessions: use everything
        return hist
    idx = session['channel_data'][ch].get('hold_index')
    return [] if idx is None else hist[idx:]


def _seconds_since_hold(session, timestamp):
    try:
        t = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        h = datetime.fromisoformat(session['hold_at'])
        return max(0.0, (t - h).total_seconds())
    except Exception:
        return 0.0


def _esc(s):
    return (
        str(s if s is not None else '')
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
    )

def _format_recorded_on(raw):
    """Return 'HH:MM:SS DD/MM/YYYY' from an ISO date or datetime string."""
    if not raw:
        return datetime.now().strftime('%H:%M:%S %d/%m/%Y')
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y'):
        try:
            dt = datetime.strptime(raw, fmt)
            if fmt in ('%Y-%m-%d', '%d/%m/%Y'):
                now = datetime.now()
                dt = dt.replace(hour=now.hour, minute=now.minute, second=now.second)
            return dt.strftime('%H:%M:%S %d/%m/%Y')
        except ValueError:
            continue
    return str(raw)


def append_archive_csv(data):
    """Archive every raw Modbus read (all channels) for traceability."""
    file_exists = os.path.isfile(ARCHIVE_LOG_FILE)
    fieldnames = ['Timestamp'] + [f'Ch_{i}' for i in range(1, TOTAL_CHANNELS + 1)]

    row = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    row.update(data)

    try:
        with open(ARCHIVE_LOG_FILE, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
    except Exception as exc:
        print(f"[ARCHIVE WARN] {exc}")


def log_fieldnames(channels):
    """Wide layout: one column per channel for pressure, unit, drop and result."""
    fields = ['Timestamp', 'Elapsed Time']
    fields += [f'CH-{c} Pressure' for c in channels]
    fields += [f'CH-{c} Unit' for c in channels]
    fields.append('Test Status')
    fields += [f'CH-{c} Pressure Drop' for c in channels]
    fields.append('Max Allowable Pressure Drop')
    fields += [f'CH-{c} Result' for c in channels]
    fields.append('Medium')
    return fields


def build_log_row(channels, samples):
    """samples: {channel: sample_dict} for ONE reading -> one CSV row."""
    first = next(iter(samples.values()))
    row = {
        'Timestamp': first['timestamp'],
        'Elapsed Time': first['elapsed_time'],
        'Test Status': first['status'],
        'Max Allowable Pressure Drop': first['max_allowable_pressure_drop'],
        'Medium': first['medium'],
    }
    for ch in channels:
        s = samples.get(ch)
        row[f'CH-{ch} Pressure'] = s['pressure'] if s else ''
        row[f'CH-{ch} Unit'] = s['unit'] if s else ''
        row[f'CH-{ch} Pressure Drop'] = (
            s['pressure_drop'] if s and s['pressure_drop'] is not None else ''
        )
        row[f'CH-{ch} Result'] = s['result'] if s else ''
    return row


def write_session_log(session, row):
    """Append one reading (all channels side by side) to the session CSV."""
    file_exists = os.path.isfile(session['log_file'])
    with open(session['log_file'], 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=log_fieldnames(session['channels']))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _parse_offset(v):
    """Offset entered in the channel pop-up (signed number); 0 if missing/invalid."""
    try:
        f = float(v)
        return f if math.isfinite(f) else 0.0
    except (TypeError, ValueError):
        return 0.0

def read_abb_data():
    """Read all 6 channels from the ABB RVG 200."""
    cfg = get_settings()
    client = ModbusTcpClient(cfg['ip_address'], port=cfg['port'], timeout=3)
    readings = {}

    if not client.connect():
        return None

    try:
        for channel in range(1, TOTAL_CHANNELS + 1):
            start_addr = (channel - 1) * 2
            try:
                response = client.read_holding_registers(
                    address=start_addr, count=2, device_id=cfg['device_id']
                )
            except Exception:
                readings[f'Ch_{channel}'] = None
                continue

            if not response.isError():
                reg1, reg2 = response.registers
                raw_bytes = struct.pack('>HH', reg2, reg1)
                float_val = struct.unpack('>f', raw_bytes)[0]

                if (
                    not math.isfinite(float_val)
                    or float_val <= -99990
                    or float_val >= 99990
                ):
                    readings[f'Ch_{channel}'] = None
                else:
                    readings[f'Ch_{channel}'] = round(float_val, 2)
            else:
                readings[f'Ch_{channel}'] = None
    finally:
        client.close()

    return readings

# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------
def create_session(config):
    now = datetime.now()
    session_id = now.strftime('%Y%m%d_%H%M%S')

    # Selected channels (default: all)
    raw_channels = config.get('channels') or list(range(1, TOTAL_CHANNELS + 1))
    channels = set()
    for c in raw_channels:
        try:
            ci = int(c)
            if 1 <= ci <= TOTAL_CHANNELS:
                channels.add(ci)
        except (TypeError, ValueError):
            continue
    channels = sorted(channels)
    if not channels:
        channels = [1]

    # Per-channel full-scale rating (e.g. {"1": "20K", "2": "20K", ...})
    input_ratings = config.get('input_ratings') or {}
    if isinstance(input_ratings, str):
        try:
            input_ratings = json.loads(input_ratings)
        except Exception:
            input_ratings = {}

    channel_units = config.get('channel_units') or {}
    channel_offsets = config.get('channel_offsets') or {}
    default_unit = config.get('unit', 'psi')

    channel_data = {}
    for ch in channels:
        channel_data[ch] = {
            'start_pressure': None,
            'current_pressure': None,
            'end_pressure': None,
            'hold_index': None,
            # 'pressure_drop': 0.0,
            'pressure_drop': None,
            'history': [],
            'result': 'Pending',
            'input_full_scale': str(
                input_ratings.get(str(ch), input_ratings.get(ch, '20K'))
            ),
            'unit': str(channel_units.get(str(ch), default_unit)),
            'offset': _parse_offset(channel_offsets.get(str(ch), channel_offsets.get(ch, 0))), 
            'max_error': 0.0,
            'relative_error': 0.0,
        }

    session = {
        'session_id': session_id,
        'started_at': now.isoformat(timespec='seconds'),
        'ended_at': None,
        'status': 'Running',
        'channels': channels,
        'channel_data': channel_data,
        'uses_hold': True,
        'hold_at': None,

        # Test config
        'unit': config.get('unit', 'psi'),
        'test_medium': config.get('test_medium', 'Air'),
        'test_duration': float(config.get('test_duration_minutes', 0) or 0) * 60,
        'sample_interval': float(config.get('sample_interval', 1) or 1),
        'max_allowable_pressure_drop': float(
            config.get('max_allowable_pressure_drop', 0) or 0
        ),

        'customer_name': config.get('customer_name', ''),
        'work_order': config.get('work_order', ''),
        'operator_name': config.get('operator_name', ''),
        'test_date': config.get('test_date', now.strftime('%Y-%m-%d')),

        # NEW Weatherford-style fields
        'tools': config.get('tools', ''),
        'description': config.get('description', ''),
        'jde': config.get('jde', ''),
        'documents': config.get('documents', ''),

        # Derived
        # 'pressure_drop': 0.0,
        'pressure_drop': None,
        'current_pressure': None,
        'start_pressure': None,
        'end_pressure': None,
        'result': 'Pending',

        # Auto-stop timer
        'timer': None,

        # Files
        'log_file': os.path.join(LOG_DIR, f'{session_id}_log.csv'),
        'report_file': os.path.join(REPORT_DIR, f'{session_id}_report.html'),
    }

    with open(session['log_file'], 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=log_fieldnames(session['channels']))
        writer.writeheader()

    return session


def payload_for_session(session):
    if not session:
        return {
            'running': False,
            'status': 'Idle',
            'session_id': None,
            'channels': [],
            'channel_data': {},
            'history': [],
            'elapsed_time': '00:00:00',
            'current_pressure': None,
            'pressure_drop': None,
            'start_pressure': None,
            'end_pressure': None,
            'result': 'Pending',
            'last_error': state.get('error'),
            'session_data': {},
        }

    timer_origin = session['started_at']
    if session.get('uses_hold'):
        timer_origin = session.get('hold_at')      # None until Hold is pressed

    elapsed_seconds = 0
    if timer_origin:
        if session['status'] == 'Running':
            elapsed_seconds = (datetime.now() - datetime.fromisoformat(timer_origin)).total_seconds()
        elif session.get('ended_at'):
            elapsed_seconds = (datetime.fromisoformat(session['ended_at'])
                            - datetime.fromisoformat(timer_origin)).total_seconds()

    holding = bool(session.get('uses_hold') and session.get('hold_at')
                and session['status'] == 'Running')


    # Flat history for frontend (with Channel column)
    flat_history = []
    for ch in session['channels']:
        for sample in session['channel_data'][ch]['history']:
            row = dict(sample)
            row['channel'] = ch
            flat_history.append(row)

    flat_history.sort(key=lambda r: r.get('timestamp', ''))

    return {
        'running': state['running'],
        'status': session['status'],
        'phase': 'Holding' if holding else session['status'],
        'hold_at': session.get('hold_at'),
        'session_id': session['session_id'],
        'channels': session['channels'],
        'channel_data': {
            str(ch): {
                'start_pressure': d['start_pressure'],
                'current_pressure': d['current_pressure'],
                'end_pressure': d['end_pressure'],
                'pressure_drop': d['pressure_drop'],
                'result': d['result'],
                'input_full_scale': d['input_full_scale'],
                'unit': d.get('unit', session['unit']),
                'max_error': d['max_error'],
                'relative_error': d['relative_error'],
                'history': d['history'],
            }
            for ch, d in session['channel_data'].items()
        },
        'history': flat_history,
        'elapsed_time': format_elapsed(elapsed_seconds),
        'current_pressure': session['current_pressure'],
        'pressure_drop': session['pressure_drop'],
        'start_pressure': session['start_pressure'],
        'end_pressure': session['end_pressure'],
        'result': session['result'],
        'last_error': state.get('error'),
        'session_data': {
            'unit': session['unit'],
            'test_medium': session['test_medium'],
            'test_duration': session['test_duration'],
            'sample_interval': session['sample_interval'],
            'max_allowable_pressure_drop': session['max_allowable_pressure_drop'],
            'customer_name': session['customer_name'],
            'work_order': session['work_order'],
            'operator_name': session['operator_name'],
            'test_date': session['test_date'],
            'tools': session['tools'],
            'description': session['description'],
            'jde': session['jde'],
            'documents': session['documents'],
        },
    }


def update_session_from_reading(session, reading):
    """Dispatch each channel reading to the matching channel slot."""
    now = datetime.now()
    elapsed_seconds = (
        now - datetime.fromisoformat(session['started_at'])
    ).total_seconds()

    unit = session['unit']
    max_drop = session['max_allowable_pressure_drop']
    medium = session['test_medium']
    holding = bool(session.get('hold_at'))

    samples = {}

    for ch in session['channels']:
        value = reading.get(f'Ch_{ch}')

        if not isinstance(value, (int, float)):
            continue  # skip N/A / ERR

        slot = session['channel_data'][ch]
        value = round(value + slot.get('offset', 0.0), 2)

        if holding and slot['hold_index'] is None:   # channel had no reading at Hold click
            slot['hold_index'] = len(slot['history'])
            slot['start_pressure'] = value

        slot['current_pressure'] = value
        if holding:
            slot['end_pressure'] = value
            slot['pressure_drop'] = round(slot['start_pressure'] - value, 2)
        else:
            slot['end_pressure'] = None
            slot['pressure_drop'] = None

        sample = {
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S'),
            'elapsed_time': format_elapsed(elapsed_seconds),
            'pressure': value,
            'unit': slot.get('unit', unit),
            'status': 'Holding' if holding else session['status'],
            'pressure_drop': slot['pressure_drop'],
            'max_allowable_pressure_drop': max_drop,
            'result': slot['result'],
            'medium': medium,
        }

        slot['history'].append(sample)
        samples[ch] = sample    

    if samples:                        # <-- one row per reading, after the loop
        write_session_log(session, build_log_row(session['channels'], samples))

    # Update session-level summary (based on first channel)
    first_ch = session['channels'][0]
    slot = session['channel_data'][first_ch]
    session['current_pressure'] = slot['current_pressure']
    session['start_pressure'] = slot['start_pressure']
    session['end_pressure'] = slot['end_pressure']
    session['pressure_drop'] = slot['pressure_drop']

    append_archive_csv(reading)



def stop_session(reason='Manual Stop'):
    global state

    with state_lock:
        session = state['session']

        if session is None:
            return None

        timer = session.get('timer')

        if timer is not None:
            try:
                timer.cancel()
            except Exception:
                pass

            session['timer'] = None

        if session['status'] != 'Running':
            return session

        if session['ended_at'] is None:
            session['ended_at'] = datetime.now().isoformat(timespec='seconds')

        session['status'] = 'Completed'

        # ---------------------------------------------------------
        # Finalize each channel
        # ---------------------------------------------------------
        overall_result = 'PASS'
        has_valid_test = False
        held = bool(session.get('hold_at'))

        for ch in session['channels']:
            slot = session['channel_data'][ch]
            if not held:                       # Stop pressed without Hold: nothing to evaluate
                slot['start_pressure'] = None
                slot['end_pressure'] = None
                slot['pressure_drop'] = None
                slot['max_error'] = 0.0
                slot['relative_error'] = 0.0
                slot['result'] = 'NO HOLD'
                overall_result = 'NO HOLD'
                continue

            hist = test_history(session, ch)   # Hold -> Stop only

            # -----------------------------------------------------
            # No valid RVG200 pressure data received
            # -----------------------------------------------------
            if not hist or slot['start_pressure'] is None:
                slot['current_pressure'] = None
                slot['end_pressure'] = None
                slot['pressure_drop'] = None
                slot['max_error'] = 0.0
                slot['relative_error'] = 0.0
                slot['result'] = 'NO DATA'

                overall_result = 'NO DATA'
                continue

            # -----------------------------------------------------
            # Only one valid pressure reading
            # -----------------------------------------------------
            if len(hist) < 2:
                slot['current_pressure'] = hist[-1]['pressure']
                slot['end_pressure'] = hist[-1]['pressure']
                slot['pressure_drop'] = None
                slot['max_error'] = 0.0
                slot['relative_error'] = 0.0
                slot['result'] = 'INSUFFICIENT DATA'

                if overall_result == 'PASS':
                    overall_result = 'INSUFFICIENT DATA'

                # Update history
                hist[-1]['status'] = 'Completed'
                hist[-1]['result'] = slot['result']

                continue

            # -----------------------------------------------------
            # Valid test data exists
            # -----------------------------------------------------
            has_valid_test = True

            slot['current_pressure'] = hist[-1]['pressure']
            slot['end_pressure'] = hist[-1]['pressure']

            start = slot['start_pressure']
            end = slot['end_pressure']

            # Calculate pressure drop
            slot['pressure_drop'] = round(start - end, 2)

            # -----------------------------------------------------
            # Calculate maximum error
            # -----------------------------------------------------
            valid_pressures = [
                sample['pressure']
                for sample in hist
                if isinstance(sample.get('pressure'), (int, float))
                and math.isfinite(sample['pressure'])
            ]

            if valid_pressures:
                max_err = max(
                    abs(start - pressure)
                    for pressure in valid_pressures
                )
            else:
                max_err = 0.0

            slot['max_error'] = round(max_err, 2)

            slot['relative_error'] = round(
                (max_err / start * 100) if start != 0 else 0.0,
                2
            )

            # -----------------------------------------------------
            # PASS / FAIL
            # Only valid pressure data can produce PASS/FAIL
            # -----------------------------------------------------
            if slot['pressure_drop'] <= session['max_allowable_pressure_drop']:
                slot['result'] = 'PASS'
            else:
                slot['result'] = 'FAIL'

            if slot['result'] == 'FAIL':
                overall_result = 'FAIL'

            # Update last history row
            hist[-1]['status'] = 'Completed'
            hist[-1]['result'] = slot['result']

        # ---------------------------------------------------------
        # Never allow PASS when there is no valid test data
        # ---------------------------------------------------------
        if not has_valid_test:
            if overall_result == 'PASS':
                overall_result = 'NO DATA'

        # ---------------------------------------------------------
        # Update session result
        # ---------------------------------------------------------
        session['result'] = overall_result
        session['reason'] = reason

        state['running'] = False
        state['error'] = None

    print(
        f"[TEST STOPPED] "
        f"Session={session['session_id']} "
        f"Reason={reason}"
    )

    try:
        generate_report_file(session)
    except Exception as exc:
        print(f"[REPORT WARN] {exc}")

    return session


def generate_report_file(session):
    if not session:
        return None

    unit = session.get('unit', 'psi')
    channels = session.get('channels', [])

    # -------- Per-channel test summary blocks --------
    test_blocks = []
    for idx, ch in enumerate(channels, start=1):
        slot = session['channel_data'][ch]
        result_class = 'pass' if slot['result'] == 'PASS' else 'fail'

        # --- Time info for this channel's test ---
        hist = test_history(session, ch)
        if hist:
            first = hist[0]
            last = hist[-1]
            started_at_str = first['timestamp'].split(' ')[1]
            ended_at_str   = last['timestamp'].split(' ')[1]
            try:
                t0 = datetime.fromisoformat(session['started_at'])
                off_start = (
                    datetime.strptime(first['timestamp'], '%Y-%m-%d %H:%M:%S') - t0
                ).total_seconds() / 60
                off_end = (
                    datetime.strptime(last['timestamp'], '%Y-%m-%d %H:%M:%S') - t0
                ).total_seconds() / 60
            except Exception:
                off_start = off_end = 0
            started_offset = f"+{off_start:.2f} m"
            ended_offset   = f"+{off_end:.2f} m"
        else:
            started_at_str = ended_at_str = '--'
            started_offset = ended_offset = ''

        test_blocks.append(f"""
        <div class="test-block">
            <div class="test-title">
                Test {idx} &hellip; <span class="{result_class}">{_esc(slot['result'])}</span>
            </div>
            <table class="kv">
                <tr><td class="k">Input</td><td class="v">{_esc(slot['input_full_scale'])}</td></tr>
                <tr><td class="k">Start</td><td class="v">{_esc(started_at_str)} ({_esc(started_offset)})</td></tr>
                <tr><td class="k">End</td><td class="v">{_esc(ended_at_str)} ({_esc(ended_offset)})</td></tr>
                <tr><td class="k">Test duration</td><td class="v">{format_elapsed(session['test_duration'])}</td></tr>
                <tr><td class="k">Start value</td><td class="v">{_fmt(slot['start_pressure'])} {_esc(unit)}</td></tr>
                <tr><td class="k">End value</td><td class="v">{_fmt(slot['end_pressure'])} {_esc(unit)}</td></tr>
                <tr><td class="k">Max. error</td><td class="v">{_fmt(slot['max_error'])} {_esc(unit)}</td></tr>
                <tr><td class="k">Relative error</td><td class="v">&plusmn;{slot['relative_error']:.2f} %</td></tr>
            </table>
        </div>
        """)

    tests_html = "\n".join(test_blocks) if test_blocks else \
        '<div style="color:#6b7280;padding:10px;">No channel data.</div>'

    # -------- Chart data (all channels on ONE report graph) --------
    chart_datasets = []
    colors = [
        '#d97706',  # CH1
        '#2563eb',  # CH2
        '#16a34a',  # CH3
        '#dc2626',  # CH4
        '#7c3aed',  # CH5
        '#0891b2',  # CH6
    ]

    # Find the largest history length
    max_labels = max(
        (len(session['channel_data'][ch]['history']) for ch in channels),
        default=0
    )

    # Use the elapsed-time labels from the longest history
    labels = []

    if max_labels > 0:
        longest_channel = max(
            channels,
            key=lambda ch: len(session['channel_data'][ch]['history'])
        )

        labels = [
            sample.get('elapsed_time', '')
            for sample in session['channel_data'][longest_channel]['history']
        ]

    # Create one dataset for EVERY selected channel
    for ch in channels:
        slot = session['channel_data'][ch]
        history = slot.get('history', [])
   

        pressures = [
            sample.get('pressure')
            for sample in history
        ]

        # Pad shorter channels so every dataset has the same
        # number of points as the common label array.
        if len(pressures) < max_labels:
            pressures.extend(
                [None] * (max_labels - len(pressures))
            )


                # Hold / Stop markers (index 0 is the zero-anchor point, hence +1)
        marker = {}
        if session.get('uses_hold') and slot.get('hold_index') is not None and history:
            marker = {
                'holdIdx': slot['hold_index'],     
                'holdVal': slot['start_pressure'],
                'stopIdx': len(history) - 1, 
                'stopVal': slot['end_pressure'],
                'unit': unit,
            }

        chart_datasets.append({
            'label': f'CH-{ch}',
            'data': pressures,
            'borderColor': colors[(ch - 1) % len(colors)],
            'backgroundColor': 'transparent',
            'borderWidth': 2,
            'pointRadius': 0,
            'pointHoverRadius': 4,
            'cubicInterpolationMode': 'monotone',
            'spanGaps': True,
            'fill': False,
            **marker,
        })

    print(
        f"[REPORT CHART] Channels={channels}, "
        f"Labels={len(labels)}, "
        f"Datasets={len(chart_datasets)}, "
        f"Points={[len(ds['data']) for ds in chart_datasets]}"
    )

    labels_json = json.dumps(labels)
    datasets_json = json.dumps(chart_datasets)

    # -------- Combined data log table --------
    combined = []
    for ch in channels:
        for s in session['channel_data'][ch]['history']:
            combined.append((s['timestamp'], ch, s))

    combined.sort(key=lambda r: r[0])

    table_rows = []
    for ts, ch, s in combined:
        table_rows.append(
            "<tr>"
            f"<td>{_esc(ts)}</td>"
            f"<td>{_esc(s['elapsed_time'])}</td>"
            f"<td>CH-{ch}</td>"
            f"<td>{s['pressure']}</td>"
            f"<td>{_esc(s['unit'])}</td>"
            f"<td>{s['pressure_drop']}</td>"
            f"<td>{_esc(s['result'])}</td>"
            "</tr>"
        )
    table_html = "\n".join(table_rows) if table_rows else \
        '<tr><td colspan="7" style="text-align:center;color:#6b7280;padding:16px;">No data.</td></tr>'

    duration_text = "00:00:00"
    duration_origin = (
        session.get('hold_at') if session.get('uses_hold') else session.get('started_at')
    )
    if duration_origin and session.get('ended_at'):
        try:
            duration_text = format_elapsed(
                (datetime.fromisoformat(session['ended_at'])
                 - datetime.fromisoformat(duration_origin)).total_seconds()
            )
        except Exception:
            pass

    overall_class = 'pass' if session.get('result') == 'PASS' else 'fail'

    # -------- Build replacements --------
    html = _report_template()
    replacements = {
        '__CUSTOMER__': _esc(session.get('customer_name', '')),
        '__WORK_ORDER__': _esc(session.get('work_order', '')),
        '__TOOLS__': _esc(session.get('tools', '')),
        '__DESCRIPTION__': _esc(session.get('description', '')),
        '__JDE__': _esc(session.get('jde', '')),
        '__DOCUMENTS__': _esc(session.get('documents', '')),
        '__TECHNICIAN__': _esc(session.get('operator_name', '')),
        '__RECORDED_ON__': _esc(_format_recorded_on(session.get('test_date'))),
        '__SESSION_ID__': _esc(session.get('session_id', '')),
        '__OVERALL_RESULT__': _esc(session.get('result', '')),
        '__OVERALL_CLASS__': overall_class,
        '__DURATION__': duration_text,
        '__UNIT__': _esc(unit),
        '__TEST_BLOCKS__': tests_html,
        '__TABLE_ROWS__': table_html,
        '__LABELS_JSON__': labels_json,
        '__DATASETS_JSON__': datasets_json,
        '__LOGO__': get_logo_data_uri(),
    }
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)

    report_file = session.get('report_file') or os.path.join(
        REPORT_DIR, f"{session['session_id']}_report.html"
    )
    session['report_file'] = report_file

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(html)

    return report_file


def _report_template():
    """Weatherford-style pressure test report — matches reference image."""
    return r"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Pressure Test Report - __SESSION_ID__</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js"></script>
<style>
    * { box-sizing: border-box; }
    html, body {
        margin: 0;
        padding: 0;
        font-family: Arial, Helvetica, sans-serif;
        background: #fff;
        color: #000;
    }
    .report {
        max-width: 1180px;
        margin: 20px auto;
        border: 1.5px solid #1f2937;
        background: #fff;
        font-size: 12px;
    }

    /* ================= HEADER (chart-left / tests-right) ================= */
    .main {
        display: grid;
        grid-template-columns: 1.75fr 1fr;
        min-height: 480px;
        border-bottom: 1.5px solid #1f2937;
    }

    .chart-pane {
        padding: 12px 14px;
        border-right: 1.5px solid #1f2937;
        position: relative;
    }
    .chart-pane .axis-label {
        position: absolute;
        top: 8px;
        left: 14px;
        font-size: 11px;
        color: #374151;
    }
    .chart-wrap {
        position: relative;
        height: 440px;
        width: 100%;
    }

    /* ================= TEST BLOCKS (right side) ================= */
    .tests-pane {
        padding: 10px 14px;
        overflow: hidden;
    }

    .test-block {
        padding: 8px 0 14px;
        border-bottom: 1px dashed #cbd5e1;
    }
    .test-block:last-child { border-bottom: none; }

    .test-title {
        font-weight: 700;
        font-size: 13px;
        margin-bottom: 6px;
    }
    .test-title .pass { color: #047857; font-weight: 700; }
    .test-title .fail { color: #b91c1c; font-weight: 700; }

    table.kv {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
    }
    table.kv td {
        padding: 2px 4px;
        vertical-align: top;
    }
    table.kv td.k {
        color: #374151;
        white-space: nowrap;
        padding-right: 10px;
        width: 1%;
    }
    table.kv td.v {
        text-align: left;
    }

    /* ================= FOOTER (branding + signature) ================= */
    .footer {
        display: grid;
        grid-template-columns: 1fr 1fr;
        font-size: 12px;
    }

    .footer-left {
        padding: 14px 18px;
        border-right: 1.5px solid #1f2937;
        display: flex;
        gap: 16px;
    }
    .wf-logo {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        min-width: 200px;
    }
    .wf-logo .mark {
        width: 42px;
        height: 42px;
        margin-bottom: 4px;
    }
    .wf-logo .name {
        font-size: 24px;
        font-weight: 900;
        letter-spacing: -0.5px;
        color: #000;
        line-height: 1;
    }
    .wf-logo .addr {
        margin-top: 6px;
        font-size: 11px;
        color: #374151;
        line-height: 1.35;
    }
    .wf-logo .version {
        margin-top: 24px;
        font-size: 9px;
        color: #6b7280;
    }

    .footer-left .fields {
        flex: 1;
        display: flex;
        flex-direction: column;
        gap: 4px;
        font-size: 12px;
    }
    .footer-left .fields .row {
        display: flex;
        gap: 8px;
    }
    .footer-left .fields .row .k {
        color: #374151;
        min-width: 80px;
    }
    .footer-left .fields .row .v {
        font-weight: 600;
    }

    .footer-right {
        padding: 14px 18px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .footer-right .fields {
        display: flex;
        flex-direction: column;
        gap: 4px;
        font-size: 12px;
    }
    .footer-right .fields .row {
        display: flex;
        gap: 8px;
    }
    .footer-right .fields .row .k {
        color: #374151;
        min-width: 80px;
    }
    .footer-right .fields .row .v { font-weight: 600; }

    .signature {
        margin-top: 18px;
        font-size: 12px;
    }
    .signature .label { color: #374151; }
    .signature .line {
        display: inline-block;
        border-bottom: 1px solid #1f2937;
        min-width: 180px;
        margin-left: 6px;
        vertical-align: bottom;
    }

    .footer-bottom {
        grid-column: 1 / -1;
        border-top: 1.5px solid #1f2937;
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 9px;
        color: #6b7280;
        padding: 5px 10px;
    }

    .wf-logo .wf-img {
        width: 170px;
        height: auto;
        display: block;
    }

    .report-actions { max-width: 1180px; margin: 20px auto 0; display: flex; justify-content: flex-end; gap: 10px; }
    .report-actions button { font-family: inherit; font-size: 13px; font-weight: 600; padding: 8px 18px; border: 1px solid #1f2937; border-radius: 4px; background: #fff; color: #1f2937; cursor: pointer; }
    .report-actions button.primary { background: #1f2937; color: #fff; }
    .report-actions button:hover { opacity: 0.85; }
    .report-actions + .report { margin-top: 10px; }

    @media print {
        body { margin: 0; }
        .report { margin: 0; border-width: 1px; }
        .no-print { display: none !important; }
    }
</style>
</head>
<body>

<div class="report-actions no-print" data-no-export="true">
    <button type="button" class="primary" id="saveReportBtn">Save Report</button>
    <button type="button" id="printReportBtn">Print</button>
</div>

<div class="report">

    <!-- ============ MAIN: chart + test blocks ============ -->
    <div class="main">

        <div class="chart-pane">
            <div class="chart-wrap">
                <canvas id="pressureChart"></canvas>
            </div>
        </div>

        <div class="tests-pane">
            __TEST_BLOCKS__
        </div>
    </div>

    <!-- ============ FOOTER: branding + signature ============ -->
    <div class="footer">

        <div class="footer-left">
            <div class="wf-logo">
                <img class="wf-img" src="__LOGO__" alt="Weatherford">
                <div class="addr">
                    Weatherford<br/>
                    Abu Dhabi<br/>
                    UAE
                </div>
            </div>

            <div class="fields">
                <div class="row"><span class="k">Customer</span><span class="v">:&nbsp;__CUSTOMER__</span></div>
                <div class="row"><span class="k">Work Order</span><span class="v">:&nbsp;__WORK_ORDER__</span></div>
                <div class="row"><span class="k">Tools</span><span class="v">:&nbsp;__TOOLS__</span></div>
                <div class="row"><span class="k">Description</span><span class="v">:&nbsp;__DESCRIPTION__</span></div>
            </div>
        </div>

        <div class="footer-right">
            <div class="fields">
                <div class="row"><span class="k">JDE</span><span class="v">:&nbsp;__JDE__</span></div>
                <div class="row"><span class="k">Documents</span><span class="v">:&nbsp;__DOCUMENTS__</span></div>
                <div class="row"><span class="k">Technician</span><span class="v">:&nbsp;__TECHNICIAN__</span></div>
                <div class="row"><span class="k">Recorded on</span><span class="v">:&nbsp;__RECORDED_ON__</span></div>
            </div>
            <div class="signature">
                <span class="label">Signature :</span><span class="line"></span>
            </div>
        </div>
    </div>
</div>

<script>
const labels   = __LABELS_JSON__;
const datasets = __DATASETS_JSON__;
console.log("REPORT LABELS:", labels.length);
console.log("REPORT DATASETS:", datasets);

/* Highlight the last data point on each dataset (like the reference) */
datasets.forEach(ds => {
    if (!ds.data || !ds.data.length) return;
    const lastIdx = ds.data.length - 1;
    ds.pointRadius = ds.data.map((_, i) => (i === lastIdx || i === ds.holdIdx || i === ds.stopIdx) ? 4 : 0);
    ds.pointBackgroundColor = ds.borderColor;
    ds.pointBorderColor = '#fff';
    ds.pointBorderWidth = 1.5;
});

/* Value boxes at the Hold point (start pressure) and Stop point (end pressure) */
const holdStopLabels = {
    id: 'holdStopLabels',
    afterDatasetsDraw(chart) {
        const ctx = chart.ctx, area = chart.chartArea;
        const items = [];

        chart.data.datasets.forEach((ds, di) => {
            const meta = chart.getDatasetMeta(di);
            [['holdIdx', 'holdVal'], ['stopIdx', 'stopVal']].forEach(([iKey, vKey]) => {
                const idx = ds[iKey], val = ds[vKey];
                if (idx === undefined || val === null || val === undefined) return;
                const pt = meta.data[idx];
                if (!pt) return;

                const text = val + ' ' + String(ds.unit || 'psi').toUpperCase();
                ctx.save();
                ctx.font = '10px Arial, sans-serif';
                const w = ctx.measureText(text).width + 8, h = 15;
                ctx.restore();

                const gap = 14 + di * 20;                       // stack channels so boxes don't overlap
                const bx = Math.min(Math.max(pt.x - w / 2, area.left + 2), area.right - w - 2);
                let by = pt.y - gap - h;
                if (by < area.top + 2) by = pt.y + gap;         // no room above: draw below
                items.push({ pt, text, w, h, bx, by, color: ds.borderColor });
            });
        });

        ctx.save();
        ctx.lineWidth = 1;
        items.forEach(it => {                                   // pass 1: leader lines
            ctx.strokeStyle = it.color;
            ctx.beginPath();
            ctx.moveTo(it.bx + it.w / 2, it.by < it.pt.y ? it.by + it.h : it.by);
            ctx.lineTo(it.pt.x, it.pt.y);
            ctx.stroke();
        });
        ctx.font = '10px Arial, sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        items.forEach(it => {                                   // pass 2: value boxes on top
            ctx.fillStyle = '#ffffff';
            ctx.fillRect(it.bx, it.by, it.w, it.h);
            ctx.strokeStyle = it.color;
            ctx.strokeRect(it.bx, it.by, it.w, it.h);
            ctx.fillStyle = '#111827';
            ctx.fillText(it.text, it.bx + it.w / 2, it.by + it.h / 2 + 0.5);
        });
        ctx.restore();
    }
};

new Chart(document.getElementById('pressureChart'), {
    type: 'line',
    plugins: [holdStopLabels],
    data: { labels, datasets },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,

        plugins: {
            legend: {
                display: true,
                position: 'top',
                align: 'start',
                labels: {
                    boxWidth: 10,
                    boxHeight: 10,
                    usePointStyle: true,
                    pointStyle: 'rect',
                    font: { size: 11 },
                    padding: 12
                }
            },
            tooltip: {
                enabled: true
            }
        },

        scales: {
            x: {
                grid: {
                    color: '#e5e7eb',
                    drawTicks: true
                },
                ticks: {
                    color: '#374151',
                    font: { size: 10 },
                    maxTicksLimit: 8,
                    callback: function(v, i) {
                        return this.getLabelForValue(v)
                            .replace(':', 'm ')
                            .replace(/^00m/, '0s');
                    }
                },
                border: {
                    color: '#1f2937'
                }
            },

            y: {
                beginAtZero: false,
                grace: '5%',
                grid: {
                    color: '#e5e7eb'
                },
                ticks: {
                    color: '#374151',
                    font: { size: 10 },
                    maxTicksLimit: 8
                },
                border: {
                    color: '#1f2937'
                }
            }
        }
    }
});

document.getElementById('printReportBtn').addEventListener('click', function () {
    window.print();
});


async function buildPdfBlob() {
    const el = document.querySelector('.report');
    const canvas = await html2canvas(el, {
        scale: 2,
        backgroundColor: '#ffffff',
        useCORS: true,
        ignoreElements: function (n) {
            return n.classList && n.classList.contains('no-print');
        }
    });

    const { jsPDF } = window.jspdf;
    const pdf = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
    const pageW = pdf.internal.pageSize.getWidth();
    const pageH = pdf.internal.pageSize.getHeight();
    const margin = 8;

    const ratio = Math.min(
        (pageW - margin * 2) / canvas.width,
        (pageH - margin * 2) / canvas.height
    );
    const w = canvas.width * ratio;
    const h = canvas.height * ratio;

    pdf.addImage(canvas.toDataURL('image/jpeg', 0.95), 'JPEG',
                 (pageW - w) / 2, margin, w, h);
    return pdf.output('blob');
}

document.getElementById('saveReportBtn').addEventListener('click', async function () {
    const btn = this;
    const suggestedName = '__SESSION_ID___report.pdf';
    let handle = null;

    // Open the Save As dialog FIRST (must happen right after the click,
    // before the slow PDF rendering, or the browser blocks it).
    if (window.showSaveFilePicker) {
        try {
            handle = await window.showSaveFilePicker({
                suggestedName: suggestedName,
                types: [{
                    description: 'PDF document',
                    accept: { 'application/pdf': ['.pdf'] }
                }]
            });
        } catch (err) {
            if (err && err.name === 'AbortError') return;  // user cancelled
            handle = null;  // picker unavailable -> fall back below
        }
    }

    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Generating PDF...';

    try {
        const blob = await buildPdfBlob();

        if (handle) {
            const writable = await handle.createWritable();
            await writable.write(blob);
            await writable.close();
        } else {
            // Fallback: normal download
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = suggestedName;
            document.body.appendChild(a);
            a.click();
            a.remove();
            setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
        }
    } catch (err) {
        console.error(err);
        alert('Could not save the PDF: ' + err.message);
    } finally {
        btn.disabled = false;
        btn.textContent = originalText;
    }
});

</script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Monitoring thread
# ---------------------------------------------------------------------------
def monitoring_loop():
    while True:
        with state_lock:
            if not state['running']:
                break
            session = state['session']
            if not session or session['status'] != 'Running':
                break
            sample_interval = max(0.1, float(session.get('sample_interval', 1.0)))

        try:
            reading = read_abb_data()

            with state_lock:
                if not state['running']:
                    break
                session = state['session']
                if not session or session['status'] != 'Running':
                    break

                if reading is None:
                    state['error'] = f"Unable to read data from RVG 200 at {get_settings()['ip_address']}."
                else:
                    update_session_from_reading(session, reading)
                    state['error'] = None

        except Exception as exc:
            with state_lock:
                state['error'] = str(exc)

        with state_lock:
            if not state['running']:
                break

        time.sleep(sample_interval)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')


def automatic_stop_session(session_id):
    with state_lock:
        session = state.get('session')
        if not session or session.get('session_id') != session_id:
            return
        if not state.get('running') or session.get('status') != 'Running':
            return

    print(f"[TIMER] Test duration reached for session {session_id}")
    stop_session('Timer-Based Automatic Stop')


@app.route('/api/start', methods=['POST'])
def api_start():
    payload = request.get_json(silent=True) or {}
    if not payload:
        return jsonify({'error': 'No configuration received.'}), 400

    with state_lock:
        if state['running']:
            return jsonify({'error': 'A test is already running.'}), 400

        session = create_session(payload)
        state['session'] = session
        state['running'] = True
        state['error'] = None

        state['thread'] = threading.Thread(target=monitoring_loop, daemon=True)
        state['thread'].start()

    return jsonify({
        'status': 'started',
        'session_id': session['session_id'],
        'channels': session['channels'],
        'test_duration': session['test_duration'],
    })

@app.route('/api/hold', methods=['POST'])
def api_hold():
    with state_lock:
        session = state['session']
        if not session or not state['running'] or session['status'] != 'Running':
            return jsonify({'error': 'No running test to hold.'}), 400
        if session.get('hold_at'):
            return jsonify({'error': 'Hold has already been applied.'}), 400

        session['hold_at'] = datetime.now().isoformat(timespec='seconds')

        for ch in session['channels']:
            slot = session['channel_data'][ch]
            hist = slot['history']
            if hist:
                slot['hold_index'] = len(hist) - 1
                slot['start_pressure'] = hist[-1]['pressure']
                slot['end_pressure'] = hist[-1]['pressure']
                slot['pressure_drop'] = 0.0
                hist[-1]['pressure_drop'] = 0.0
                hist[-1]['status'] = 'Holding'

        first = session['channel_data'][session['channels'][0]]
        session['start_pressure'] = first['start_pressure']
        session['end_pressure'] = first['end_pressure']
        session['pressure_drop'] = first['pressure_drop']

        test_duration = session.get('test_duration', 0)
        if test_duration > 0:                      # auto-stop now counts from Hold
            timer = threading.Timer(test_duration, automatic_stop_session,
                                    args=(session['session_id'],))
            timer.daemon = True
            session['timer'] = timer
            timer.start()

        hold_at = session['hold_at']
    return jsonify({'status': 'holding', 'hold_at': hold_at})


@app.route('/api/stop', methods=['POST'])
def api_stop():
    with state_lock:
        if not state['session']:
            return jsonify({'status': 'idle'})

    session = stop_session('Manual Stop')
    return jsonify({
        'status': 'stopped',
        'session_id': session['session_id'] if session else None,
    })


@app.route('/api/reset', methods=['POST'])
def api_reset():
    with state_lock:
        session = state['session']
        thread = state.get('thread')

        if session is not None:
            timer = session.get('timer')
            if timer is not None:
                try:
                    timer.cancel()
                except Exception:
                    pass
                session['timer'] = None

        state['running'] = False      # monitoring_loop exits on its next check
        state['session'] = None       # drops samples, hold time, results
        state['error'] = None
        state['thread'] = None

    if thread is not None and thread.is_alive():
        thread.join(timeout=2)        # outside the lock so the loop can finish

    return jsonify({'status': 'reset'})


@app.route('/api/state')
def api_state():
    with state_lock:
        session = state['session']
        payload = payload_for_session(session)
    return jsonify(payload)

@app.route('/api/export_csv')
def api_export_csv():
    with state_lock:
        session = state['session']

    if not session:
        return jsonify({'error': 'No test data available.'}), 400

    channels = session['channels']

    # Group samples taken at the same moment into one row
    # (n handles two readings landing in the same second)
    groups = {}
    for ch in channels:
        seen = {}
        for s in session['channel_data'][ch]['history']:
            key = (s['timestamp'], s['elapsed_time'])
            n = seen.get(key, 0)
            seen[key] = n + 1
            groups.setdefault((s['timestamp'], n, s['elapsed_time']), {})[ch] = s

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=log_fieldnames(channels))
    writer.writeheader()
    for key in sorted(groups, key=lambda k: (k[0], k[1])):
        writer.writerow(build_log_row(channels, groups[key]))

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f"{session['session_id']}_log.csv",
    )



@app.route('/api/import_csv', methods=['POST'])
def api_import_csv():
    """Import a combined CSV (with Channel column) and rebuild a session."""
    if 'file' not in request.files:
        return jsonify({'error': 'No CSV file selected.'}), 400

    file = request.files['file']
    if not file.filename or not file.filename.lower().endswith('.csv'):
        return jsonify({'error': 'Please select a CSV file.'}), 400

    try:
        content = file.read().decode('utf-8-sig')
        reader = csv.DictReader(io.StringIO(content))
        fieldnames = reader.fieldnames or []

        if 'Channel' in fieldnames:                 # old long format
            rows = list(reader)
        else:                                       # new side-by-side format
            ch_nums = sorted(
                int(m.group(1)) for m in
                (re.match(r'^CH-(\d+) Pressure$', h) for h in fieldnames) if m
            )
            rows = []
            for r in reader:
                for n in ch_nums:
                    if not (r.get(f'CH-{n} Pressure') or '').strip():
                        continue
                    rows.append({
                        'Timestamp': r.get('Timestamp', ''),
                        'Elapsed Time': r.get('Elapsed Time', '00:00:00'),
                        'Channel': str(n),
                        'Pressure': r.get(f'CH-{n} Pressure', ''),
                        'Unit': r.get(f'CH-{n} Unit', ''),
                        'Test Status': r.get('Test Status', 'Completed'),
                        'Pressure Drop': r.get(f'CH-{n} Pressure Drop', ''),
                        'Max Allowable Pressure Drop': r.get('Max Allowable Pressure Drop', ''),
                        'Result': r.get(f'CH-{n} Result', 'Pending'),
                        'Medium': r.get('Medium', ''),
                    })

        per_channel = {}
        unit = 'psi'
        medium = 'Air'
        max_drop = 0.0
        first_ts = None
        last_ts = None

        for row in rows:
            try:
                ch = int(row.get('Channel', '1'))
            except ValueError:
                ch = 1

            if ch not in per_channel:
                per_channel[ch] = {
                    'start_pressure': None,
                    'current_pressure': None,
                    'end_pressure': None,
                    'pressure_drop': 0.0,
                    'history': [],
                    'result': 'Pending',
                    'input_full_scale': '20K',
                    'max_error': 0.0,
                    'relative_error': 0.0,
                }

            pressure_value = row.get('Pressure', '').strip()
            if not pressure_value:
                continue

            try:
                pressure = float(pressure_value)
            except ValueError:
                continue

            try:
                pressure_drop = float(row.get('Pressure Drop', 0) or 0)
            except ValueError:
                pressure_drop = 0.0

            try:
                max_drop = float(row.get('Max Allowable Pressure Drop', 0) or 0)
            except ValueError:
                pass

            unit = row.get('Unit', unit) or unit
            medium = row.get('Medium', medium) or medium

            ts = row.get('Timestamp', '')
            if first_ts is None:
                first_ts = ts
            last_ts = ts

            slot = per_channel[ch]
            if slot['start_pressure'] is None:
                slot['start_pressure'] = pressure
            slot['current_pressure'] = pressure
            slot['end_pressure'] = pressure
            slot['pressure_drop'] = pressure_drop
            slot['history'].append({
                'timestamp': ts,
                'elapsed_time': row.get('Elapsed Time', '00:00:00'),
                'pressure': pressure,
                'unit': unit,
                'status': row.get('Test Status', 'Completed'),
                'pressure_drop': pressure_drop,
                'max_allowable_pressure_drop': max_drop,
                'result': row.get('Result', 'Pending'),
                'medium': medium,
            })

        if not per_channel:
            return jsonify({
                'error': 'No valid pressure data was found in the CSV file.'
            }), 400

        # Compute per-channel errors and results
        overall_result = 'PASS'
        for ch, slot in per_channel.items():
            start = slot['start_pressure'] or 0
            hist = slot['history']
            if hist:
                max_err = max(abs(start - s['pressure']) for s in hist)
                slot['max_error'] = round(max_err, 2)
                slot['relative_error'] = round(
                    (max_err / start * 100) if start else 0.0, 2
                )
            slot['pressure_drop'] = round(start - (slot['end_pressure'] or 0), 2)
            slot['result'] = 'PASS' if slot['pressure_drop'] <= max_drop else 'FAIL'
            if slot['result'] == 'FAIL':
                overall_result = 'FAIL'

        # Parse timestamps
        def parse_ts(ts):
            try:
                return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                return datetime.now()

        start_dt = parse_ts(first_ts)
        end_dt = parse_ts(last_ts)

        session_id = f"IMPORTED_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        channels = sorted(per_channel.keys())

        imported_session = {
            'session_id': session_id,
            'started_at': start_dt.isoformat(timespec='seconds'),
            'ended_at': end_dt.isoformat(timespec='seconds'),
            'status': 'Imported',
            'channels': channels,
            'channel_data': per_channel,

            'unit': unit,
            'test_medium': medium,
            'test_duration': max(0, (end_dt - start_dt).total_seconds()),
            'sample_interval': 1,
            'max_allowable_pressure_drop': max_drop,

            'customer_name': '',
            'work_order': '',
            'operator_name': '',
            'test_date': start_dt.strftime('%Y-%m-%d'),
            'tools': '',
            'description': '',
            'jde': '',
            'documents': '',

            'pressure_drop': per_channel[channels[0]]['pressure_drop'],
            'current_pressure': per_channel[channels[0]]['current_pressure'],
            'start_pressure': per_channel[channels[0]]['start_pressure'],
            'end_pressure': per_channel[channels[0]]['end_pressure'],
            'result': overall_result,

            'timer': None,
            'log_file': '',
            'report_file': '',
        }

        with state_lock:
            state['session'] = imported_session
            state['running'] = False
            state['error'] = None

        # Try to generate the report immediately
        try:
            generate_report_file(imported_session)
        except Exception as exc:
            print(f"[IMPORT REPORT WARN] {exc}")

        total_rows = sum(len(s['history']) for s in per_channel.values())

        return jsonify({
            'status': 'imported',
            'session_id': session_id,
            'count': total_rows,
        })

    except Exception as exc:
        return jsonify({'error': f'Failed to import CSV: {exc}'}), 500


@app.route('/api/report')
def api_report():
    with state_lock:
        session = state['session']

    if not session:
        return jsonify({'error': 'No test session found.'}), 400

    if not session.get('ended_at'):
        return jsonify({
            'error': 'The test is still running. Stop or complete the test before opening the report.'
        }), 400

    try:
        report_path = generate_report_file(session)
        # return jsonify({'report_url': f'/report/{os.path.basename(report_path)}'})
        return jsonify({
            'report_url': f'/report/{os.path.basename(report_path)}?t={int(time.time())}'
        })
    except Exception as exc:
        return jsonify({'error': f'Failed to generate report: {exc}'}), 500


@app.route('/report/<path:filename>')
def serve_report(filename):
    report_path = os.path.join(REPORT_DIR, filename)

    if os.path.exists(report_path):
        response = send_file(report_path)
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response

    return 'Report not found', 404


@app.route('/api/settings', methods=['GET'])
def api_get_settings():
    return jsonify(get_settings())


@app.route('/api/settings', methods=['POST'])
def api_save_settings():
    with state_lock:
        if state['running']:
            return jsonify({'error': 'Stop the running test before changing connection settings.'}), 400

    clean, err = validate_settings(request.get_json(silent=True))
    if err:
        return jsonify({'error': err}), 400

    try:
        save_settings_to_disk(clean)
    except Exception as exc:
        return jsonify({'error': f'Could not save settings: {exc}'}), 500

    global settings
    with settings_lock:
        settings = clean
    return jsonify({'status': 'saved', **clean})


@app.route('/api/test_connection', methods=['POST'])
def api_test_connection():
    """Try the values currently typed in the UI (saved or not)."""
    with state_lock:
        if state['running']:
            return jsonify({'ok': False, 'error': 'Cannot test while a test is running.'}), 400

    payload = request.get_json(silent=True)
    clean, err = validate_settings(payload) if payload else (get_settings(), None)
    if err:
        return jsonify({'ok': False, 'error': err}), 400

    client = ModbusTcpClient(clean['ip_address'], port=clean['port'], timeout=3)
    try:
        if not client.connect():
            return jsonify({'ok': False, 'error': f"Could not connect to {clean['ip_address']}:{clean['port']}."})
        resp = client.read_holding_registers(address=0, count=2, device_id=clean['device_id'])
        if resp.isError():
            return jsonify({'ok': False, 'error': 'Connected, but the device did not respond to a read. Check the Device ID.'})
        return jsonify({'ok': True, 'message': f"Connected to {clean['ip_address']}:{clean['port']}."})
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)})
    finally:
        client.close()


@app.route('/api/read', methods=['POST'])
def api_read():
    """Read-only snapshot of current pressure for the requested channels.

    Does not create a session, start monitoring, log, or change any state.
    While a test is running it returns the latest values the monitor already
    holds (so the instrument isn't opened by a second connection).
    """
    payload = request.get_json(silent=True) or {}
    wanted = []
    for c in payload.get('channels') or []:
        try:
            ci = int(c)
        except (TypeError, ValueError):
            continue
        if 1 <= ci <= TOTAL_CHANNELS and ci not in wanted:
            wanted.append(ci)
    wanted.sort()
    if not wanted:
        return jsonify({'ok': False, 'error': 'Select at least one channel.'}), 400
    offsets = payload.get('channel_offsets') or {}

    with state_lock:
        session = state.get('session')
        if state['running'] and session and session.get('status') == 'Running':
            values = {}
            for ch in wanted:
                slot = session['channel_data'].get(ch)
                values[str(ch)] = slot.get('current_pressure') if slot else None
            return jsonify({'ok': True, 'source': 'running_test', 'values': values,
                            'read_at': datetime.now().strftime('%H:%M:%S')})

    cfg = get_settings()
    if not cfg.get('ip_address'):
        return jsonify({'ok': False, 'error': 'No instrument IP set. Enter it under Connection and click Save.'}), 400

    try:
        reading = read_abb_data()
    except Exception as exc:
        return jsonify({'ok': False, 'error': f'Read failed: {exc}'}), 502
    if reading is None:
        return jsonify({'ok': False,
                        'error': f"Unable to connect to RVG 200 at {cfg['ip_address']}:{cfg['port']}."}), 502

    values = {}
    for ch in wanted:
        v = reading.get(f'Ch_{ch}')
        if isinstance(v, (int, float)):
            v = round(v + _parse_offset(offsets.get(str(ch), 0)), 2)   # apply channel offset
        values[str(ch)] = v
    return jsonify({'ok': True, 'source': 'instrument', 'values': values,
                    'read_at': datetime.now().strftime('%H:%M:%S')})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)