import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, render_template, jsonify, request, Response
from core.queue_manager import QueueManager
from core.result_store import ResultStore
from workers.worker_pool import WorkerPool
from config.settings import settings
import json

app = Flask(__name__)

queue_manager = QueueManager()
result_store = ResultStore()
worker_pool = WorkerPool()

@app.route('/')
def dashboard():
    """Page d'accueil du tableau de bord."""
    stats = {
        'queues': queue_manager.get_queue_stats(),
        'tasks': {
            'pending': result_store.count_pending(),
            'completed': result_store.count_completed(),
            'failed': result_store.count_failed()
        },
        'workers': worker_pool.get_worker_stats()
    }
    return render_template('dashboard.html', stats=stats)

@app.route('/task/<task_id>')
def task_detail(task_id):
    """Détails d'une tâche spécifique."""
    task = result_store.get_result(task_id)
    if not task:
        return render_template('error.html', message="Tâche non trouvée"), 404
    return render_template('task_detail.html', task=task)

@app.route('/queues')
def queue_status():
    """État des files d'attente."""
    queues = queue_manager.get_all_queue_status()
    return render_template('queue_status.html', queues=queues)

@app.route('/api/queues', methods=['GET'])
def api_queues():
    """API pour obtenir l'état des files."""
    return jsonify(queue_manager.get_all_queue_status())

@app.route('/api/tasks', methods=['GET'])
def api_tasks():
    """API pour obtenir la liste des tâches."""
    status_filter = request.args.get('status', 'all')
    limit = int(request.args.get('limit', 100))
    
    if status_filter == 'pending':
        tasks = result_store.get_pending_tasks(limit)
    elif status_filter == 'completed':
        tasks = result_store.get_completed_tasks(limit)
    elif status_filter == 'failed':
        tasks = result_store.get_failed_tasks(limit)
    else:
        tasks = result_store.get_recent_tasks(limit)
    
    return jsonify(tasks)

@app.route('/api/task/<task_id>', methods=['GET'])
def api_task_detail(task_id):
    """API pour obtenir les détails d'une tâche."""
    task = result_store.get_result(task_id)
    if not task:
        return jsonify({'error': 'Tâche non trouvée'}), 404
    return jsonify(task)

@app.route('/api/workers', methods=['GET'])
def api_workers():
    """API pour obtenir l'état des workers."""
    return jsonify(worker_pool.get_worker_stats())

@app.route('/api/queues/<queue_name>/tasks', methods=['GET'])
def api_queue_tasks(queue_name):
    """API pour obtenir les tâches d'une file spécifique."""
    tasks = queue_manager.get_queue_tasks(queue_name)
    return jsonify(tasks)

@app.route('/stream/logs')
def stream_logs():
    """Flux SSE pour les logs en temps réel."""
    def generate():
        # Cette implémentation nécessiterait un système de logs en temps réel
        yield "data: {}\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/api/system/stats')
def system_stats():
    """Statistiques système globales."""
    return jsonify({
        'memory_usage': result_store.get_memory_usage(),
        'queue_counts': queue_manager.get_queue_counts(),
        'worker_status': worker_pool.get_worker_status()
    })

@app.errorhandler(404)
def not_found(error):
    """Gestionnaire d'erreur 404."""
    return render_template('error.html', message="Page non trouvée"), 404

@app.errorhandler(500)
def server_error(error):
    """Gestionnaire d'erreur 500."""
    return render_template('error.html', message="Erreur interne du serveur"), 500

if __name__ == '__main__':
    app.run(
        host=settings.WEB_HOST,
        port=settings.WEB_PORT,
        debug=settings.DEBUG
    )