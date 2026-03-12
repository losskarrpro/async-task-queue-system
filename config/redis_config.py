import os
from typing import Optional
from dataclasses import dataclass
from redis import Redis
from redis.exceptions import ConnectionError

from utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class RedisConfig:
    """Configuration pour la connexion Redis."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    decode_responses: bool = True
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    max_connections: int = 10
    health_check_interval: int = 30
    
    @classmethod
    def from_env(cls) -> "RedisConfig":
        """Crée une configuration à partir des variables d'environnement."""
        return cls(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=os.getenv("REDIS_DECODE_RESPONSES", "true").lower() == "true",
            socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT", 5.0)),
            socket_connect_timeout=float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", 5.0)),
            max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", 10)),
            health_check_interval=int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", 30))
        )
    
    def to_dict(self) -> dict:
        """Retourne la configuration sous forme de dictionnaire."""
        config_dict = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "decode_responses": self.decode_responses,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "max_connections": self.max_connections,
            "health_check_interval": self.health_check_interval
        }
        if self.password:
            config_dict["password"] = self.password
        return config_dict


def create_redis_client(config: Optional[RedisConfig] = None) -> Optional[Redis]:
    """Crée et retourne un client Redis connecté."""
    if config is None:
        config = RedisConfig.from_env()
    
    try:
        client = Redis(**config.to_dict())
        # Test de la connexion
        client.ping()
        logger.info(f"Connecté à Redis sur {config.host}:{config.port}, base de données {config.db}")
        return client
    except ConnectionError as e:
        logger.warning(f"Impossible de se connecter à Redis: {e}")
        return None
    except Exception as e:
        logger.error(f"Erreur inattendue lors de la connexion à Redis: {e}")
        return None


def check_redis_health(client: Redis) -> bool:
    """Vérifie la santé de la connexion Redis."""
    try:
        return client.ping()
    except Exception:
        return False


def get_redis_info(client: Redis) -> Optional[dict]:
    """Récupère les informations du serveur Redis."""
    try:
        return client.info()
    except Exception as e:
        logger.error(f"Impossible de récupérer les informations Redis: {e}")
        return None


# Instance de configuration globale
redis_config = RedisConfig.from_env()

# Client Redis global (None si Redis n'est pas disponible)
redis_client: Optional[Redis] = None

def init_redis() -> bool:
    """Initialise la connexion Redis globale."""
    global redis_client
    redis_client = create_redis_client()
    return redis_client is not None


def get_redis() -> Optional[Redis]:
    """Retourne le client Redis global."""
    return redis_client