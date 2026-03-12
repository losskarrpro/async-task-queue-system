import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch, MagicMock
import pytest
from click.testing import CliRunner
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from cli.main import cli
from cli.commands.submit import submit_task
from cli.commands.monitor import monitor_tasks
from cli.commands.queue import queue_status, queue_clear
from core.queue_manager import QueueManager
from core.result_store import ResultStore
from workers.worker_pool import WorkerPool
from core.task import Task, TaskPriority


class TestCliIntegration:
    """Tests d'intégration pour la CLI."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def mock_queue_manager(self):
        with patch('cli.commands.submit.QueueManager') as mock:
            instance = Mock()
            instance.submit_task = AsyncMock()
            instance.get_queue_info = AsyncMock(return_value={'size': 5, 'pending': 3})
            instance.clear_queue = AsyncMock()
            instance.get_task = AsyncMock()
            mock.return_value = instance
            yield instance

    @pytest.fixture
    def mock_result_store(self):
        with patch('cli.commands.submit.ResultStore') as mock:
            instance = Mock()
            instance.get_result = AsyncMock()
            instance.get_all_results = AsyncMock()
            mock.return_value = instance
            yield instance

    @pytest.fixture
    def mock_worker_pool(self):
        with patch('cli.commands.monitor.WorkerPool') as mock:
            instance = Mock()
            instance.get_worker_stats = AsyncMock()
            mock.return_value = instance
            yield instance

    @pytest.mark.asyncio
    async def test_submit_command(self, mock_queue_manager, mock_result_store):
        """Test de la commande submit."""
        runner = CliRunner()
        
        # Test avec des paramètres minimaux
        result = runner.invoke(cli, [
            'submit', 
            '--task-type', 'example',
            '--data', '{"param1": "value1"}'
        ])
        assert result.exit_code == 0
        mock_queue_manager.submit_task.assert_called_once()
        
        # Test avec priorité
        result = runner.invoke(cli, [
            'submit',
            '--task-type', 'example',
            '--data', '{"param1": "value1"}',
            '--priority', 'HIGH'
        ])
        assert result.exit_code == 0
        
        # Test avec queue spécifique
        result = runner.invoke(cli, [
            'submit',
            '--task-type', 'example',
            '--data', '{"param1": "value1"}',
            '--queue', 'priority_queue'
        ])
        assert result.exit_code == 0
        
        # Test avec données invalides
        result = runner.invoke(cli, [
            'submit',
            '--task-type', 'example',
            '--data', 'invalid_json'
        ])
        assert result.exit_code != 0

    @pytest.mark.asyncio
    async def test_monitor_command(self, mock_result_store, mock_worker_pool):
        """Test de la commande monitor."""
        runner = CliRunner()
        
        # Mock des résultats
        mock_result_store.get_all_results.return_value = {
            'task1': {'status': 'COMPLETED', 'result': 'success'},
            'task2': {'status': 'FAILED', 'error': 'Some error'}
        }
        
        mock_worker_pool.get_worker_stats.return_value = {
            'active_workers': 3,
            'idle_workers': 1,
            'total_tasks_processed': 100
        }
        
        # Test monitor all
        result = runner.invoke(cli, ['monitor', '--all'])
        assert result.exit_code == 0
        assert 'COMPLETED' in result.output
        assert 'FAILED' in result.output
        
        # Test monitor avec task_id
        mock_result_store.get_result.return_value = {
            'status': 'COMPLETED',
            'result': 'success',
            'task_id': 'task123'
        }
        
        result = runner.invoke(cli, ['monitor', '--task-id', 'task123'])
        assert result.exit_code == 0
        assert 'task123' in result.output
        
        # Test stats
        result = runner.invoke(cli, ['monitor', '--stats'])
        assert result.exit_code == 0
        assert 'active_workers' in result.output

    @pytest.mark.asyncio
    async def test_queue_command(self, mock_queue_manager):
        """Test des commandes queue."""
        runner = CliRunner()
        
        # Test queue status
        result = runner.invoke(cli, ['queue', 'status'])
        assert result.exit_code == 0
        mock_queue_manager.get_queue_info.assert_called()
        
        # Test queue status avec nom spécifique
        result = runner.invoke(cli, ['queue', 'status', '--name', 'default'])
        assert result.exit_code == 0
        
        # Test queue clear
        result = runner.invoke(cli, ['queue', 'clear', '--name', 'default', '--force'])
        assert result.exit_code == 0
        mock_queue_manager.clear_queue.assert_called_with('default')
        
        # Test queue clear sans confirmation
        result = runner.invoke(cli, ['queue', 'clear', '--name', 'default'])
        assert result.exit_code != 0

    @pytest.mark.asyncio
    async def test_submit_command_with_real_task(self):
        """Test de soumission avec une tâche réelle."""
        runner = CliRunner()
        
        with patch('cli.commands.submit.QueueManager') as mock_qm:
            mock_instance = AsyncMock()
            mock_qm.return_value = mock_instance
            
            # Créer une tâche de test
            task_data = {
                'operation': 'add',
                'numbers': [1, 2, 3, 4, 5]
            }
            
            result = runner.invoke(cli, [
                'submit',
                '--task-type', 'calculation',
                '--data', json.dumps(task_data),
                '--priority', 'MEDIUM',
                '--queue', 'math_queue'
            ])
            
            assert result.exit_code == 0
            mock_instance.submit_task.assert_called_once()
            
            # Vérifier les paramètres
            call_args = mock_instance.submit_task.call_args
            task = call_args[0][0]  # Premier argument
            queue_name = call_args[1]['queue_name']
            
            assert task.task_type == 'calculation'
            assert task.data == task_data
            assert queue_name == 'math_queue'

    @pytest.mark.asyncio
    async def test_monitor_real_time(self):
        """Test du monitoring en temps réel."""
        runner = CliRunner()
        
        with patch('cli.commands.monitor.ResultStore') as mock_rs, \
             patch('cli.commands.monitor.WorkerPool') as mock_wp:
            
            mock_rs_instance = AsyncMock()
            mock_rs.return_value = mock_rs_instance
            
            mock_wp_instance = AsyncMock()
            mock_wp.return_value = mock_wp_instance
            
            # Configurer le mock pour simuler des mises à jour
            results = [
                {'task1': {'status': 'PENDING'}},
                {'task1': {'status': 'PROCESSING'}},
                {'task1': {'status': 'COMPLETED', 'result': 15}}
            ]
            
            mock_rs_instance.get_all_results.side_effect = results
            
            # Exécuter avec l'option watch
            import threading
            import time
            
            def run_command():
                result = runner.invoke(cli, ['monitor', '--watch', '--interval', '1'])
            
            thread = threading.Thread(target=run_command)
            thread.daemon = True
            thread.start()
            
            # Attendre un peu puis arrêter
            time.sleep(2.5)
            # Le test vérifie juste que la commande s'exécute sans erreur

    def test_cli_help(self):
        """Test que l'aide fonctionne pour toutes les commandes."""
        runner = CliRunner()
        
        commands = [
            [],
            ['submit', '--help'],
            ['monitor', '--help'],
            ['queue', '--help'],
            ['queue', 'status', '--help'],
            ['queue', 'clear', '--help']
        ]
        
        for cmd in commands:
            result = runner.invoke(cli, cmd)
            assert result.exit_code == 0, f"Command {cmd} failed: {result.output}"
            assert 'Usage:' in result.output or 'Commands:' in result.output

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test de la gestion des erreurs dans la CLI."""
        runner = CliRunner()
        
        # Test avec un type de tâche inconnu
        with patch('cli.commands.submit.QueueManager') as mock_qm:
            mock_instance = AsyncMock()
            mock_instance.submit_task.side_effect = ValueError("Unknown task type")
            mock_qm.return_value = mock_instance
            
            result = runner.invoke(cli, [
                'submit',
                '--task-type', 'unknown_type',
                '--data', '{}'
            ])
            
            assert result.exit_code != 0
            assert 'Error' in result.output or 'error' in result.output.lower()
        
        # Test avec store inaccessible
        with patch('cli.commands.monitor.ResultStore') as mock_rs:
            mock_instance = AsyncMock()
            mock_instance.get_all_results.side_effect = ConnectionError("Cannot connect to store")
            mock_rs.return_value = mock_instance
            
            result = runner.invoke(cli, ['monitor', '--all'])
            assert result.exit_code != 0

    @pytest.mark.asyncio
    async def test_output_formats(self):
        """Test des différents formats de sortie."""
        runner = CliRunner()
        
        with patch('cli.commands.submit.QueueManager') as mock_qm, \
             patch('cli.commands.monitor.ResultStore') as mock_rs:
            
            mock_qm_instance = AsyncMock()
            mock_qm_instance.submit_task.return_value = 'task123'
            mock_qm.return_value = mock_qm_instance
            
            mock_rs_instance = AsyncMock()
            mock_rs_instance.get_result.return_value = {
                'task_id': 'task123',
                'status': 'COMPLETED',
                'result': {'sum': 15},
                'created_at': '2023-01-01T00:00:00',
                'completed_at': '2023-01-01T00:00:05'
            }
            mock_rs.return_value = mock_rs_instance
            
            # Test format JSON
            result = runner.invoke(cli, [
                'submit',
                '--task-type', 'test',
                '--data', '{}',
                '--output', 'json'
            ])
            assert result.exit_code == 0
            try:
                json_output = json.loads(result.output)
                assert 'task_id' in json_output
            except json.JSONDecodeError:
                pytest.fail("Output is not valid JSON")
            
            # Test format text (par défaut)
            result = runner.invoke(cli, ['monitor', '--task-id', 'task123'])
            assert result.exit_code == 0
            assert 'task123' in result.output
            assert 'COMPLETED' in result.output
            
            # Test format détaillé
            result = runner.invoke(cli, ['monitor', '--task-id', 'task123', '--verbose'])
            assert result.exit_code == 0
            assert 'created_at' in result.output
            assert 'completed_at' in result.output

    def test_autocomplete(self):
        """Test de l'autocomplétion (si implémentée)."""
        runner = CliRunner()
        
        # Test que les commandes principales existent
        result = runner.invoke(cli, ['submit'])
        assert result.exit_code != 0  # Manque des arguments requis
        
        result = runner.invoke(cli, ['monitor'])
        assert result.exit_code == 0  # Peut s'exécuter sans arguments
        
        result = runner.invoke(cli, ['queue'])
        assert result.exit_code != 0  # Sous-commande requise

    @pytest.mark.asyncio
    async def test_concurrent_submissions(self):
        """Test de soumissions concurrentes via CLI."""
        runner = CliRunner()
        
        with patch('cli.commands.submit.QueueManager') as mock_qm:
            mock_instance = AsyncMock()
            mock_qm.return_value = mock_instance
            
            # Simuler plusieurs soumissions rapides
            import concurrent.futures
            
            def submit_one(i):
                return runner.invoke(cli, [
                    'submit',
                    '--task-type', f'test_{i}',
                    '--data', json.dumps({'index': i}),
                    '--output', 'json'
                ])
            
            # Soumettre 5 tâches
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(submit_one, i) for i in range(5)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
            
            # Vérifier que toutes ont réussi
            for result in results:
                assert result.exit_code == 0
            
            # Vérifier le nombre d'appels
            assert mock_instance.submit_task.call_count == 5