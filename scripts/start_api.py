#!/usr/bin/env python3
"""
Script pour démarrer le serveur API du système de file de tâches asynchrones.
"""

import argparse
import os
import sys
import uvicorn
from pathlib import Path

# Ajouter le répertoire parent au chemin pour les imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def validate_port(port: int) -> int:
    """Valide que le port est dans la plage autorisée (7500-7600)."""
    if not (7500 <= port <= 7600):
        raise argparse.ArgumentTypeError(
            f"Le port doit être compris entre 7500 et 7600 (inclus). Port fourni: {port}"
        )
    return port

def main():
    """Point d'entrée principal du script."""
    parser = argparse.ArgumentParser(
        description="Démarrer l'API du système de file de tâches asynchrones"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Adresse d'écoute de l'API (défaut: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=validate_port,
        default=7500,
        help="Port d'écoute de l'API (7500-7600, défaut: 7500)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="Activer le rechargement automatique (développement uniquement)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Niveau de journalisation (défaut: info)",
    )
    
    args = parser.parse_args()
    
    print(f"Démarrage de l'API sur {args.host}:{args.port}")
    print(f"Mode rechargement: {'Activé' if args.reload else 'Désactivé'}")
    print(f"Niveau de journalisation: {args.log_level}")
    print("Appuyez sur Ctrl+C pour arrêter le serveur\n")
    
    try:
        uvicorn.run(
            "api.app:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            log_level=args.log_level,
            access_log=True,
        )
    except KeyboardInterrupt:
        print("\nArrêt du serveur API")
        sys.exit(0)
    except Exception as e:
        print(f"Erreur lors du démarrage de l'API: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()