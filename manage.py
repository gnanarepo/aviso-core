#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


def _sanitize_settings_databases(settings_module_path=None):
    """Import the settings module and ensure DATABASES entries have NAME when using Postgres.
    If a Postgres entry has no NAME, switch that alias to a local sqlite fallback file.
    """
    try:
        import importlib
        import os as _os
        if not settings_module_path:
            settings_module_path = _os.environ.get('DJANGO_SETTINGS_MODULE')
        if not settings_module_path:
            return
        mod = importlib.import_module(settings_module_path)
        if not hasattr(mod, 'DATABASES'):
            return
        DBS = getattr(mod, 'DATABASES') or {}
        base_dir = _os.path.dirname(_os.path.abspath(__file__))
        for alias, cfg in list(DBS.items()):
            if isinstance(cfg, dict):
                engine = cfg.get('ENGINE', '')
                name = cfg.get('NAME')
                if engine and ('postgres' in engine or 'postgresql' in engine) and not name:
                    DBS[alias] = {
                        'ENGINE': 'django.db.backends.sqlite3',
                        'NAME': _os.path.join(base_dir, f'{alias}_fallback.sqlite3'),
                    }
        setattr(mod, 'DATABASES', DBS)
    except Exception:
        # Keep failure silent — we don't want to break manage commands during sanitization
        return


def main():
    """Run administrative tasks."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'aviso_core.settings')
    os.environ.setdefault('DJANGO_ENVIRONMENT', 'development')
    # Sanitize settings early to avoid Django raising ImproperlyConfigured for missing DB names
    # _sanitize_settings_databases(os.environ.get('DJANGO_SETTINGS_MODULE'))
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
