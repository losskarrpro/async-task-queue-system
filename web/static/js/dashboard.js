const API_BASE_URL = 'http://localhost:7500';
const REFRESH_INTERVAL = 5000;

let statsChart = null;
let tasksTable = null;
let refreshIntervalId = null;

document.addEventListener('DOMContentLoaded', function() {
    initDataTables();
    loadAllData();
    setupEventListeners();
    startAutoRefresh();
});

function initDataTables() {
    tasksTable = $('#tasks-table').DataTable({
        order: [[3, 'desc']],
        pageLength: 10,
        columns: [
            { data: 'id' },
            { data: 'task_type' },
            { data: 'status' },
            { data: 'created_at' },
            { data: 'queue_type' },
            { data: 'priority' },
            {
                data: null,
                render: function(data) {
                    return data.result ? 'Oui' : 'Non';
                }
            },
            {
                data: null,
                render: function(data) {
                    return `<button class="btn btn-sm btn-info view-task" data-id="${data.id}">Voir</button>`;
                }
            }
        ]
    });
}

async function loadAllData() {
    try {
        await Promise.all([
            loadTasks(),
            loadQueues(),
            loadStats(),
            loadWorkers()
        ]);
    } catch (error) {
        showError('Erreur de chargement des données');
    }
}

async function loadTasks() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks`);
        const data = await response.json();
        
        tasksTable.clear();
        tasksTable.rows.add(data.tasks);
        tasksTable.draw();
        
        updateTaskCounts(data);
    } catch (error) {
        console.error('Erreur lors du chargement des tâches:', error);
    }
}

function updateTaskCounts(data) {
    const pending = data.tasks.filter(t => t.status === 'pending').length;
    const running = data.tasks.filter(t => t.status === 'running').length;
    const completed = data.tasks.filter(t => t.status === 'completed').length;
    const failed = data.tasks.filter(t => t.status === 'failed').length;
    
    document.getElementById('pending-count').textContent = pending;
    document.getElementById('running-count').textContent = running;
    document.getElementById('completed-count').textContent = completed;
    document.getElementById('failed-count').textContent = failed;
    document.getElementById('total-count').textContent = data.tasks.length;
}

async function loadQueues() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/queues`);
        const data = await response.json();
        
        const container = document.getElementById('queues-container');
        container.innerHTML = '';
        
        data.queues.forEach(queue => {
            const queueCard = createQueueCard(queue);
            container.appendChild(queueCard);
        });
    } catch (error) {
        console.error('Erreur lors du chargement des files:', error);
    }
}

function createQueueCard(queue) {
    const card = document.createElement('div');
    card.className = 'col-md-4 mb-3';
    
    const bgColor = queue.size > 10 ? 'bg-warning' : 
                   queue.size > 0 ? 'bg-info' : 'bg-success';
    
    card.innerHTML = `
        <div class="card">
            <div class="card-header ${bgColor} text-white">
                <h5 class="mb-0">${queue.name}</h5>
            </div>
            <div class="card-body">
                <p class="card-text">
                    <strong>Taille:</strong> ${queue.size}<br>
                    <strong>Type:</strong> ${queue.type}<br>
                    <strong>Workers:</strong> ${queue.active_workers}
                </p>
                <div class="progress">
                    <div class="progress-bar" role="progressbar" 
                         style="width: ${Math.min(queue.size * 10, 100)}%">
                    </div>
                </div>
            </div>
        </div>
    `;
    
    return card;
}

async function loadStats() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/stats`);
        const data = await response.json();
        
        updateStatsChart(data);
        updateStatsNumbers(data);
    } catch (error) {
        console.error('Erreur lors du chargement des statistiques:', error);
    }
}

function updateStatsChart(data) {
    const ctx = document.getElementById('stats-chart').getContext('2d');
    
    if (statsChart) {
        statsChart.destroy();
    }
    
    statsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.hourly_stats.map(s => s.hour),
            datasets: [
                {
                    label: 'Tâches complétées',
                    data: data.hourly_stats.map(s => s.completed),
                    borderColor: 'rgb(75, 192, 192)',
                    tension: 0.1
                },
                {
                    label: 'Tâches échouées',
                    data: data.hourly_stats.map(s => s.failed),
                    borderColor: 'rgb(255, 99, 132)',
                    tension: 0.1
                }
            ]
        },
        options: {
            responsive: true,
            plugins: {
                title: {
                    display: true,
                    text: 'Activité horaire'
                }
            }
        }
    });
}

function updateStatsNumbers(data) {
    document.getElementById('avg-processing-time').textContent = 
        data.avg_processing_time.toFixed(2);
    document.getElementById('success-rate').textContent = 
        `${data.success_rate.toFixed(1)}%`;
    document.getElementById('tasks-per-hour').textContent = 
        data.tasks_per_hour.toFixed(1);
}

async function loadWorkers() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/workers`);
        const data = await response.json();
        
        const container = document.getElementById('workers-list');
        container.innerHTML = '';
        
        data.workers.forEach(worker => {
            const workerItem = createWorkerItem(worker);
            container.appendChild(workerItem);
        });
    } catch (error) {
        console.error('Erreur lors du chargement des workers:', error);
    }
}

function createWorkerItem(worker) {
    const item = document.createElement('div');
    item.className = 'list-group-item';
    
    const statusClass = worker.status === 'active' ? 'badge-success' : 
                       worker.status === 'idle' ? 'badge-warning' : 'badge-danger';
    
    item.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div>
                <h6 class="mb-0">Worker ${worker.id}</h6>
                <small class="text-muted">${worker.current_task || 'Aucune tâche'}</small>
            </div>
            <span class="badge ${statusClass}">${worker.status}</span>
        </div>
    `;
    
    return item;
}

function setupEventListeners() {
    document.getElementById('refresh-btn').addEventListener('click', loadAllData);
    
    document.getElementById('auto-refresh').addEventListener('change', function(e) {
        if (e.target.checked) {
            startAutoRefresh();
        } else {
            stopAutoRefresh();
        }
    });
    
    document.getElementById('tasks-table').addEventListener('click', function(e) {
        if (e.target.classList.contains('view-task')) {
            const taskId = e.target.dataset.id;
            viewTaskDetails(taskId);
        }
    });
}

async function viewTaskDetails(taskId) {
    try {
        const response = await fetch(`${API_BASE_URL}/api/tasks/${taskId}`);
        const data = await response.json();
        
        const modal = new bootstrap.Modal(document.getElementById('task-details-modal'));
        const modalBody = document.getElementById('task-details-body');
        
        modalBody.innerHTML = `
            <dl class="row">
                <dt class="col-sm-3">ID:</dt>
                <dd class="col-sm-9">${data.id}</dd>
                
                <dt class="col-sm-3">Type:</dt>
                <dd class="col-sm-9">${data.task_type}</dd>
                
                <dt class="col-sm-3">Statut:</dt>
                <dd class="col-sm-9">
                    <span class="badge ${getStatusBadgeClass(data.status)}">
                        ${data.status}
                    </span>
                </dd>
                
                <dt class="col-sm-3">Créée le:</dt>
                <dd class="col-sm-9">${new Date(data.created_at).toLocaleString()}</dd>
                
                <dt class="col-sm-3">Priorité:</dt>
                <dd class="col-sm-9">${data.priority}</dd>
                
                ${data.result ? `
                <dt class="col-sm-3">Résultat:</dt>
                <dd class="col-sm-9"><pre>${JSON.stringify(data.result, null, 2)}</pre></dd>
                ` : ''}
                
                ${data.error ? `
                <dt class="col-sm-3">Erreur:</dt>
                <dd class="col-sm-9 text-danger">${data.error}</dd>
                ` : ''}
            </dl>
        `;
        
        modal.show();
    } catch (error) {
        showError('Erreur lors du chargement des détails de la tâche');
    }
}

function getStatusBadgeClass(status) {
    switch(status) {
        case 'completed': return 'bg-success';
        case 'running': return 'bg-primary';
        case 'pending': return 'bg-warning';
        case 'failed': return 'bg-danger';
        default: return 'bg-secondary';
    }
}

function startAutoRefresh() {
    if (refreshIntervalId) {
        clearInterval(refreshIntervalId);
    }
    refreshIntervalId = setInterval(loadAllData, REFRESH_INTERVAL);
}

function stopAutoRefresh() {
    if (refreshIntervalId) {
        clearInterval(refreshIntervalId);
        refreshIntervalId = null;
    }
}

function showError(message) {
    const alert = document.createElement('div');
    alert.className = 'alert alert-danger alert-dismissible fade show';
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    document.querySelector('.container').prepend(alert);
    
    setTimeout(() => {
        alert.remove();
    }, 5000);
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleString();
}

window.addEventListener('beforeunload', stopAutoRefresh);