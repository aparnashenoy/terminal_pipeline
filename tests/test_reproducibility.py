"""Focused regressions; Airflow itself is verified with the README smoke commands."""
import contextlib
import io
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def read_dag(project_dir=None, source=None):
    tasks = {}

    def task(fn):
        tasks[fn.__name__] = fn
        return lambda *args, **kwargs: None

    def dag(**kwargs):
        return lambda fn: fn

    decorators = types.ModuleType('airflow.decorators')
    decorators.task, decorators.dag = task, dag
    env = dict(os.environ)
    env.pop('TERMINAL_PIPELINE_PROJECT_DIR', None)
    if project_dir is not None:
        env['TERMINAL_PIPELINE_PROJECT_DIR'] = str(project_dir)
    with patch.dict(os.environ, env, clear=True), patch.dict(sys.modules, {
        'airflow': types.ModuleType('airflow'), 'airflow.decorators': decorators,
    }):
        namespace = runpy.run_path(str(source or ROOT / 'weekly_terminal_report_dag.py'))
    return namespace, tasks


class ReproducibilityTests(unittest.TestCase):
    def test_default_follows_copied_dag_from_unrelated_cwd(self):
        with tempfile.TemporaryDirectory(prefix='dag space ') as tmp:
            copied = Path(tmp) / 'dag.py'
            shutil.copy2(ROOT / 'weekly_terminal_report_dag.py', copied)
            namespace, _ = read_dag(source=copied)
            self.assertEqual(namespace['PROJECT_DIR'], Path(tmp).resolve())

    def test_absolute_override_and_json_output(self):
        with tempfile.TemporaryDirectory(prefix='project space ') as tmp:
            namespace, tasks = read_dag(tmp)
            self.assertEqual(namespace['PROJECT_DIR'], Path(tmp).resolve())
            results = {'summary': {'window_end': '2026-01-05'}, 'by_berth': {}}
            report = Path(tasks['load'](results))
            import json
            self.assertEqual(json.loads(report.read_text()), results)
            self.assertEqual(report.parent, Path(tmp) / 'output')
            with contextlib.redirect_stdout(io.StringIO()) as output:
                tasks['notify'](str(report))
                namespace['alert_on_failure']({
                    'task_instance': types.SimpleNamespace(task_id='extract'),
                    'dag': types.SimpleNamespace(dag_id='weekly_terminal_report'),
                    'exception': RuntimeError('missing data'),
                })
            self.assertIn('Would email/Slack', output.getvalue())
            self.assertIn('no email/Slack sent', output.getvalue())

    def test_relative_override_rejected(self):
        with self.assertRaisesRegex(ValueError, 'absolute path'):
            read_dag('relative/data')

    def test_cron_wrapper_anchors_cwd_and_propagates_failure(self):
        with tempfile.TemporaryDirectory(prefix='cron space ') as tmp:
            project = Path(tmp) / 'checkout space'
            project.mkdir()
            wrapper = project / 'run_weekly_report.sh'
            shutil.copy2(ROOT / wrapper.name, wrapper)
            (project / 'weekly_report.py').write_text(
                'import os, sys\nprint(os.getcwd())\nsys.exit(7)\n'
            )
            result = subprocess.run(
                ['bash', str(wrapper)], cwd=tmp,
                env={**os.environ, 'PYTHON_BIN': sys.executable},
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 7, result.stderr)
            self.assertEqual(result.stdout.strip(), str(project))


if __name__ == '__main__':
    unittest.main()
