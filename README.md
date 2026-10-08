# Pressure Test Dashboard

Standalone Flask dashboard for the AB RVG 200 pressure test system.

## Project structure

```text
pressure_test_dashboard/
├── app.py
├── requirements.txt
├── templates/
│   └── index.html
├── test_logs/       # created automatically
└── reports/         # created automatically
```

## Run in VS Code / PowerShell

```powershell
cd pressure_test_dashboard
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

Then open:

http://127.0.0.1:5000

## AB RVG 200 connection

The current settings are preserved from the supplied code:

- IP: `192.168.1.9`
- Modbus TCP port: `502`
- Device ID: `255`
- Channels: `6`

If the AB RVG 200 uses a different IP, edit `IP_ADDRESS` in `app.py`.

## Important

The dashboard reads two Modbus registers per channel and converts them to a 32-bit floating-point value using the byte/register order in the supplied code.

The browser UI is now a separate `templates/index.html`; Flask serves it with `render_template()` instead of embedding the entire HTML inside Python.

The supplied source also had a locking pattern that could deadlock when automatic stop was triggered. The separated version uses `threading.RLock()` so the existing session logic can safely call `stop_session()`.
