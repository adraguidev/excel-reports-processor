import json
import base64
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import sys

from utils_logging import logger


def _resolve_storage_path(config_file: str) -> Path:
    """
    Determina una ruta estable para guardar credenciales.
    - En modo script usa la carpeta del módulo.
    - En ejecutable (PyInstaller) utiliza el directorio donde vive el .exe.
    """
    if getattr(sys, "frozen", False):
        base_dir = Path(sys.executable).resolve().parent
    else:
        base_dir = Path(__file__).resolve().parent
    return base_dir / config_file


class CredentialsManager:
    """Helper que persiste credenciales NTLM en disco."""

    def __init__(self, config_file: str = "credentials.json") -> None:
        self.config_path = _resolve_storage_path(config_file)
        self.credentials: dict = {}
        self._load_credentials()

    def _write_credentials(self, data: dict) -> None:
        """Guarda las credenciales en formato JSON."""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _load_credentials(self) -> None:
        """Carga credenciales desde disco (soporta formato legacy base64)."""
        if not self.config_path.exists():
            logger.info("Archivo de credenciales no encontrado, se inicia vacío")
            self.credentials = {}
            return

        try:
            raw = self.config_path.read_text(encoding="utf-8").strip()
            if not raw:
                self.credentials = {}
                return

            try:
                self.credentials = json.loads(raw)
                logger.info("Credenciales cargadas desde JSON")
            except json.JSONDecodeError:
                decoded = base64.b64decode(raw.encode("utf-8")).decode("utf-8")
                self.credentials = json.loads(decoded)
                logger.info("Credenciales convertidas desde formato base64 legacy")
                self._write_credentials(self.credentials)
        except Exception as exc:
            logger.error(f"Error cargando credenciales: {exc}")
            self.credentials = {}

    def save_credentials(self, user: str, password: str, url: Optional[str] = None) -> bool:
        """Persistir credenciales NTLM y opcionalmente la URL del servidor."""
        user = user.strip() if user else user
        if not user or not password:
            logger.warning("Intento de guardar credenciales incompletas")
            return False

        try:
            payload = {
                "ntlm_user": user,
                "ntlm_pass": password,
                "last_updated": datetime.utcnow().isoformat() + "Z",
                "version": "2.0",
            }
            if url:
                payload["server_url"] = url

            self._write_credentials(payload)
            self.credentials = payload
            logger.info(f"Credenciales NTLM guardadas para el usuario: {user}")
            return True
        except Exception as exc:
            logger.error(f"Error guardando credenciales: {exc}")
            return False

    def get_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """Retorna (usuario, contraseña) si ambas están presentes."""
        if not self.credentials:
            return None, None

        user = self.credentials.get("ntlm_user")
        password = self.credentials.get("ntlm_pass")
        if user and password:
            return user, password

        logger.warning("Credenciales incompletas encontradas")
        return None, None

    def get_server_url(self) -> Optional[str]:
        """Retorna la URL de servidor almacenada, si existe."""
        return self.credentials.get("server_url") if self.credentials else None

    def has_credentials(self) -> bool:
        """Indica si existen credenciales completas."""
        user, password = self.get_credentials()
        return bool(user) and bool(password)

    def clear_credentials(self) -> bool:
        """Elimina el archivo con las credenciales guardadas."""
        try:
            if self.config_path.exists():
                self.config_path.unlink()
            self.credentials = {}
            logger.info("Credenciales eliminadas correctamente")
            return True
        except Exception as exc:
            logger.error(f"Error eliminando credenciales: {exc}")
            return False


credentials_manager = CredentialsManager()
