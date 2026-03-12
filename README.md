# Async Task Queue System

Un système de file de tâche asynchrone en Python avec architecture modulaire, conçu pour gérer et exécuter des tâches de manière efficace et scalable.

## 📋 Fonctionnalités

- **Gestion de files multiples** : FIFO, LIFO et files à priorité
- **Workers asynchrones** : Consommation parallèle de tâches avec gestion de pool
- **Stockage des résultats** : Mémoire RAM ou Redis pour la persistance
- **Interfaces multiples** : CLI, API REST et interface web de monitoring
- **Extensible** : Architecture modulaire pour ajouter de nouveaux types de tâches
- **Monitoring complet** : Suivi en temps réel des tâches et de la file d'attente

## 🏗 Architecture

Le système est construit autour d'une architecture modulaire :

```
┌─────────────────────────────────────────────────────┐
│                    CLI / API / Web                   │
└──────────────────────────┬──────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────┐
│                 QueueManager (Core)                  │
│  - FIFO Queue      - LIFO Queue    - Priority Queue │
└─────────┬──────────────────┬────────────────────┬────┘
          │                  │                    │
┌─────────▼────┐     ┌──────▼────────┐    ┌──────▼─────┐
│  Task Store  │     │ Worker Pool   │    │ Result     │
│              │     │               │    │ Store      │
└──────────────┘     └───────────────┘    └────────────┘
```

## 📋 Prérequis

- Python 3.9+
- Redis (optionnel, pour le stockage persistant)
- pip ou poetry

## 🚀 Installation

### Installation via pip

```bash
# Clonez le repository
git clone https://github.com/votre-repo/async-task-queue-system.git
cd async-task-queue-system

# Installez les dépendances
pip install -r requirements.txt

# Pour le développement
pip install -r requirements-dev.txt
```

### Installation avec Poetry

```bash
poetry install
poetry install --with dev  # Pour les dépendances de développement
```

### Installation avec Docker

```bash
# Construisez l'image
docker build -t async-task-queue .

# Ou utilisez docker-compose
docker-compose up -d
```

## ⚙️ Configuration

Le système peut être configuré via des variables d'environnement ou le fichier `config/settings.py` :

```bash
# Configuration Redis (optionnel)
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_DB=0

# Configuration de l'API
export API_HOST=0.0.0.0
export API_PORT=7500

# Configuration Web
export WEB_HOST=0.0.0.0
export WEB_PORT=7501

# Configuration Workers
export MAX_WORKERS=4
export QUEUE_TYPE=FIFO  # FIFO, LIFO, PRIORITY
```

## 🎮 Utilisation

### Interface en ligne de commande (CLI)

Le système fournit une CLI complète pour interagir avec la file de tâches :

```bash
# Soumettre une tâche
python -m cli.main submit --type=example --data='{"message": "Hello World"}'

# Surveiller les tâches
python -m cli.main monitor

# Gérer la file d'attente
python -m cli.main queue status
python -m cli.main queue clear

# Lister les tâches
python -m cli.main list --status=pending
```

### API REST

L'API REST est disponible sur le port `7500` par défaut :

```bash
# Démarrer l'API
python scripts/start_api.py
# ou
python -m api.app
```

#### Endpoints disponibles :

- `GET /api/v1/health` - Vérifier l'état de l'API
- `POST /api/v1/tasks` - Soumettre une nouvelle tâche
- `GET /api/v1/tasks` - Lister toutes les tâches
- `GET /api/v1/tasks/{task_id}` - Récupérer une tâche spécifique
- `GET /api/v1/queue/status` - Obtenir le statut de la file d'attente
- `GET /api/v1/monitoring/stats` - Statistiques de monitoring

#### Exemple avec curl :

```bash
# Soumettre une tâche
curl -X POST http://localhost:7500/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "example_task",
    "data": {"message": "Test task"},
    "priority": 1
  }'

# Vérifier le statut
curl http://localhost:7500/api/v1/tasks/{task_id}
```

### Interface Web

L'interface web de monitoring est disponible sur le port `7501` :

```bash
# Démarrer le serveur web
python web/routes.py
```

Accédez à `http://localhost:7501` pour :
- Dashboard en temps réel
- Visualisation des tâches
- Statut de la file d'attente
- Métriques système

### Workers

Démarrer les workers pour traiter les tâches :

```bash
# Démarrer un worker
python scripts/start_workers.py --workers=4

# Démarrer avec configuration spécifique
python -m workers.worker_pool --max-workers=4 --queue-type=FIFO
```

## 🔧 Développement

### Structure du projet

```
async-task-queue-system/
├── config/              # Configuration
├── core/               # Cœur du système (QueueManager, Task, ResultStore)
├── workers/            # Workers asynchrones
├── api/                # API REST
├── cli/                # Interface en ligne de commande
├── web/                # Interface web
├── tasks/              # Définitions des tâches
├── utils/              # Utilitaires
├── tests/              # Tests unitaires et d'intégration
└── scripts/            # Scripts utilitaires
```

### Ajouter un nouveau type de tâche

1. Créez une nouvelle classe dans `tasks/` :

```python
# tasks/my_task.py
from tasks.base_task import BaseTask

class MyCustomTask(BaseTask):
    task_type = "my_custom_task"
    
    async def execute(self):
        # Implémentation de la tâche
        result = await self.process_data(self.data)
        return {"status": "completed", "result": result}
    
    async def process_data(self, data):
        # Traitement des données
        return f"Processed: {data}"
```

2. Enregistrez la tâche dans le registre :

```python
# tasks/registry.py
from tasks.my_task import MyCustomTask

TASK_REGISTRY = {
    "my_custom_task": MyCustomTask,
    # ... autres tâches
}
```

3. Utilisez la tâche via CLI ou API :

```bash
python -m cli.main submit --type=my_custom_task --data='{"param": "value"}'
```

### Exécuter les tests

```bash
# Tous les tests
pytest

# Tests spécifiques
pytest tests/test_queue_manager.py
pytest tests/test_api.py -v

# Tests avec couverture
pytest --cov=core --cov=api --cov=workers

# Tests d'intégration
pytest tests/test_integration.py
```

## 🐳 Déploiement avec Docker

### Docker simple

```bash
# Construire l'image
docker build -t async-task-queue .

# Exécuter le conteneur
docker run -p 7500:7500 -p 7501:7501 async-task-queue
```

### Docker Compose (avec Redis)

```bash
# Démarrer tous les services
docker-compose up -d

# Démarrer avec scaling
docker-compose up -d --scale worker=4

# Voir les logs
docker-compose logs -f api
docker-compose logs -f worker
```

Le fichier `docker-compose.yml` configure :
- API sur le port 7500
- Interface web sur le port 7501
- Redis pour le stockage persistant
- Multiple workers

## 📊 Monitoring

### Métriques disponibles

1. **File d'attente** :
   - Nombre de tâches en attente
   - Taille moyenne de la file
   - Temps d'attente moyen

2. **Workers** :
   - Nombre de workers actifs
   - Taux d'utilisation CPU
   - Tâches traitées par worker

3. **Tâches** :
   - Taux de réussite/échec
   - Temps d'exécution moyen
   - Distribution par type de tâche

### Intégration avec des outils externes

Le système expose des métriques au format Prometheus sur `/api/v1/monitoring/metrics` et peut être intégré avec :
- Grafana pour la visualisation
- Prometheus pour la collecte
- Sentry pour le monitoring d'erreurs

## 🔐 Sécurité

### Authentification (optionnelle)

Pour sécuriser l'API, configurez une clé secrète :

```bash
export API_SECRET_KEY=votre-cle-secrete-tres-longue
```

L'API vérifiera alors le header `X-API-Key` sur toutes les requêtes.

### Best practices

1. **Validation des données** : Toutes les entrées sont validées via Pydantic
2. **Gestion des erreurs** : Exceptions spécifiques avec messages d'erreur clairs
3. **Logging structuré** : Logs au format JSON pour une intégration facile
4. **Rate limiting** : Limitation des requêtes par IP (configurable)

## 🤝 Contribuer

Les contributions sont les bienvenues ! Veuillez suivre ces étapes :

1. Fork le projet
2. Créez une branche pour votre fonctionnalité (`git checkout -b feature/amazing-feature`)
3. Committez vos changements (`git commit -m 'Add some amazing feature'`)
4. Push vers la branche (`git push origin feature/amazing-feature`)
5. Ouvrez une Pull Request

### Guidelines de code

- Suivez le style PEP 8
- Écrivez des tests pour les nouvelles fonctionnalités
- Documentez les nouvelles API
- Mettez à jour le CHANGELOG.md

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 🆘 Support

- [Documentation détaillée](docs/)
- [Issues GitHub](https://github.com/votre-repo/async-task-queue-system/issues)
- [Exemples d'utilisation](examples/)

## 📞 Contact

Pour toute question ou support, veuillez ouvrir une issue sur GitHub ou contacter l'équipe de développement.

---

**Note** : Les ports par défaut sont 7500 pour l'API et 7501 pour l'interface web. Assurez-vous que ces ports sont disponibles ou modifiez-les dans la configuration.