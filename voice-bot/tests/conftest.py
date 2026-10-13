import os, sys, pathlib
import pytest
ROOT = pathlib.Path(__file__).resolve().parents[1]
if not (ROOT / 'enterprise').exists():
    ROOT = pathlib.Path.cwd()
sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv('DATA_DIR', str(tmp_path))
    monkeypatch.setenv('DATABASE_URL', f"sqlite+aiosqlite:///{tmp_path/'leads.db'}")
    monkeypatch.setenv('DASHBOARD_API_KEY', 'test-key')
    monkeypatch.setenv('PUBLIC_HOST', 'bot.example-client.in')
    import leads.db as d
    for name in ('_async_engine', '_async_session_factory'):
        if hasattr(d, name):
            setattr(d, name, None)
    yield
