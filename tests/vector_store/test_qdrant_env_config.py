from unittest.mock import patch

from semantica.vector_store.config import VectorStoreConfig
from semantica.vector_store.vector_store import VectorStore


def test_qdrant_environment_configuration_is_loaded():
    environment = {
        "VECTOR_STORE_DEFAULT_BACKEND": "qdrant",
        "VECTOR_STORE_DIMENSION": "768",
        "VECTOR_STORE_QDRANT_URL": "http://qdrant-mem0:6333",
        "VECTOR_STORE_QDRANT_API_KEY": "test-key",
    }

    with patch.dict("os.environ", environment, clear=True):
        config = VectorStoreConfig()

    assert config.get("default_backend") == "qdrant"
    assert config.get("dimension") == 768
    assert config.get("qdrant_url") == "http://qdrant-mem0:6333"
    assert config.get("qdrant_api_key") == "test-key"


def test_qdrant_backend_receives_environment_parameter_names():
    config = {
        "dimension": 768,
        "qdrant_url": "http://qdrant-mem0:6333",
        "qdrant_api_key": "test-key",
    }

    store = VectorStore(backend="qdrant", config=config)

    assert store._backend_store.url == "http://qdrant-mem0:6333"
    assert store._backend_store.api_key == "test-key"
