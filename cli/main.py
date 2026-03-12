#!/usr/bin/env python3
"""Point d'entrée principal de la CLI pour le système de file de tâches asynchrones."""

import asyncio
import click
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from cli.commands.submit import submit_task, submit_batch
from cli.commands.monitor import monitor_task, monitor_queue, monitor_workers
from cli.commands.queue import list_queues, purge_queue, pause_queue, resume_queue

logger = setup_logger(__name__)

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Activer le mode verbeux.')
@click.option('--config', '-c', default='config/settings.py', help='Chemin vers le fichier de configuration.')
@click.pass_context
def cli(ctx, verbose, config):
    """Système de file de tâches asynchrones - Interface en ligne de commande."""
    ctx.ensure_object(dict)
    ctx.obj['VERBOSE'] = verbose
    ctx.obj['CONFIG_PATH'] = config
    
    if verbose:
        logger.setLevel('DEBUG')
        logger.debug("Mode verbeux activé")

@cli.command()
@click.argument('task_type')
@click.argument('task_data', required=False)
@click.option('--queue', '-q', default='default', help='Nom de la file d\'attente.')
@click.option('--priority', '-p', type=int, default=5, help='Priorité de la tâche (1-10).')
@click.option('--lifo', is_flag=True, help='Utiliser le mode LIFO au lieu de FIFO.')
@click.option('--delay', '-d', type=int, default=0, help='Délai d\'exécution en secondes.')
@click.option('--timeout', '-t', type=int, default=300, help='Timeout de la tâche en secondes.')
@click.option('--max-retries', '-r', type=int, default=3, help='Nombre maximum de tentatives.')
@click.pass_context
def submit(ctx, task_type, task_data, queue, priority, lifo, delay, timeout, max_retries):
    """Soumettre une nouvelle tâche à la file d'attente."""
    try:
        result = asyncio.run(submit_task(
            task_type=task_type,
            task_data=task_data,
            queue_name=queue,
            priority=priority,
            lifo=lifo,
            delay=delay,
            timeout=timeout,
            max_retries=max_retries,
            config_path=ctx.obj['CONFIG_PATH']
        ))
        
        if result:
            click.echo(f"✓ Tâche soumise avec succès. ID: {result}")
        else:
            click.echo("✗ Échec de la soumission de la tâche.")
            
    except Exception as e:
        logger.error(f"Erreur lors de la soumission: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('batch_file', type=click.Path(exists=True))
@click.option('--queue', '-q', default='default', help='Nom de la file d\'attente.')
@click.option('--concurrent', '-c', type=int, default=10, help='Nombre de soumissions concurrentes.')
@click.pass_context
def submit_batch_cmd(ctx, batch_file, queue, concurrent):
    """Soumettre un lot de tâches depuis un fichier JSON."""
    try:
        results = asyncio.run(submit_batch(
            batch_file=batch_file,
            queue_name=queue,
            concurrent=concurrent,
            config_path=ctx.obj['CONFIG_PATH']
        ))
        
        success_count = sum(1 for r in results if r)
        click.echo(f"✓ {success_count}/{len(results)} tâches soumises avec succès.")
        
    except Exception as e:
        logger.error(f"Erreur lors de la soumission par lot: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('task_id')
@click.option('--follow', '-f', is_flag=True, help='Suivre en temps réel la progression.')
@click.option('--interval', '-i', type=float, default=2.0, help='Intervalle de rafraîchissement en secondes.')
@click.pass_context
def monitor(ctx, task_id, follow, interval):
    """Surveiller l'état d'une tâche spécifique."""
    try:
        asyncio.run(monitor_task(
            task_id=task_id,
            follow=follow,
            interval=interval,
            config_path=ctx.obj['CONFIG_PATH']
        ))
    except KeyboardInterrupt:
        click.echo("\nArrêt de la surveillance.")
    except Exception as e:
        logger.error(f"Erreur lors de la surveillance: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('queue_name', required=False)
@click.option('--all', '-a', is_flag=True, help='Afficher toutes les files d\'attente.')
@click.option('--stats', '-s', is_flag=True, help='Afficher les statistiques détaillées.')
@click.option('--follow', '-f', is_flag=True, help='Surveiller en temps réel.')
@click.option('--interval', '-i', type=float, default=5.0, help='Intervalle de rafraîchissement en secondes.')
@click.pass_context
def queue_monitor(ctx, queue_name, all, stats, follow, interval):
    """Surveiller l'état des files d'attente."""
    try:
        if not queue_name and not all:
            click.echo("Veuillez spécifier un nom de file ou utiliser --all")
            sys.exit(1)
            
        asyncio.run(monitor_queue(
            queue_name=queue_name,
            show_all=all,
            show_stats=stats,
            follow=follow,
            interval=interval,
            config_path=ctx.obj['CONFIG_PATH']
        ))
    except KeyboardInterrupt:
        click.echo("\nArrêt de la surveillance.")
    except Exception as e:
        logger.error(f"Erreur lors de la surveillance: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--follow', '-f', is_flag=True, help='Surveiller en temps réel.')
@click.option('--interval', '-i', type=float, default=5.0, help='Intervalle de rafraîchissement en secondes.')
@click.pass_context
def workers(ctx, follow, interval):
    """Surveiller l'état des workers."""
    try:
        asyncio.run(monitor_workers(
            follow=follow,
            interval=interval,
            config_path=ctx.obj['CONFIG_PATH']
        ))
    except KeyboardInterrupt:
        click.echo("\nArrêt de la surveillance.")
    except Exception as e:
        logger.error(f"Erreur lors de la surveillance: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--type', '-t', type=click.Choice(['all', 'fifo', 'lifo', 'priority']), default='all', help='Type de file à lister.')
@click.option('--active-only', '-a', is_flag=True, help='Afficher uniquement les files actives.')
@click.pass_context
def list(ctx, type, active_only):
    """Lister les files d'attente disponibles."""
    try:
        asyncio.run(list_queues(
            queue_type=type,
            active_only=active_only,
            config_path=ctx.obj['CONFIG_PATH']
        ))
    except Exception as e:
        logger.error(f"Erreur lors du listage: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('queue_name')
@click.option('--force', '-f', is_flag=True, help='Forcer la purge sans confirmation.')
@click.pass_context
def purge(ctx, queue_name, force):
    """Purger toutes les tâches d'une file d'attente."""
    try:
        asyncio.run(purge_queue(
            queue_name=queue_name,
            force=force,
            config_path=ctx.obj['CONFIG_PATH']
        ))
    except Exception as e:
        logger.error(f"Erreur lors de la purge: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('queue_name')
@click.pass_context
def pause(ctx, queue_name):
    """Mettre en pause une file d'attente."""
    try:
        asyncio.run(pause_queue(
            queue_name=queue_name,
            config_path=ctx.obj['CONFIG_PATH']
        ))
        click.echo(f"✓ File '{queue_name}' mise en pause.")
    except Exception as e:
        logger.error(f"Erreur lors de la mise en pause: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('queue_name')
@click.pass_context
def resume(ctx, queue_name):
    """Reprendre une file d'attente en pause."""
    try:
        asyncio.run(resume_queue(
            queue_name=queue_name,
            config_path=ctx.obj['CONFIG_PATH']
        ))
        click.echo(f"✓ File '{queue_name}' reprise.")
    except Exception as e:
        logger.error(f"Erreur lors de la reprise: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
@click.option('--host', '-h', default='127.0.0.1', help='Adresse IP du serveur web.')
@click.option('--port', '-p', default=7500, type=int, help='Port du serveur web (7500-7600).')
@click.pass_context
def web(ctx, host, port):
    """Lancer l'interface web de monitoring."""
    if port < 7500 or port > 7600:
        click.echo("✗ Erreur: Le port doit être compris entre 7500 et 7600")
        sys.exit(1)
    
    try:
        from scripts.start_api import start_api_server
        click.echo(f"Lancement de l'interface web sur http://{host}:{port}")
        asyncio.run(start_api_server(host=host, port=port, config_path=ctx.obj['CONFIG_PATH']))
    except KeyboardInterrupt:
        click.echo("\nArrêt du serveur web.")
    except Exception as e:
        logger.error(f"Erreur lors du lancement du serveur web: {str(e)}")
        click.echo(f"✗ Erreur: {str(e)}")
        sys.exit(1)

@cli.command()
def version():
    """Afficher la version du système."""
    try:
        import importlib.metadata
        version = importlib.metadata.version('async-task-queue-system')
        click.echo(f"async-task-queue-system v{version}")
    except:
        click.echo("async-task-queue-system (version de développement)")

def main():
    """Fonction principale d'entrée."""
    try:
        cli(obj={})
    except KeyboardInterrupt:
        click.echo("\nOpération interrompue.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erreur CLI non gérée: {str(e)}")
        click.echo(f"✗ Erreur critique: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()