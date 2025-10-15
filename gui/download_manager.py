import queue
import time

class DownloadManager:
    def __init__(self, message_queue: queue.Queue):
        self._queue = message_queue
        self._cancelled = False
        self._paused = False
        self._current_speed = 0
        self._start_time = None
        self._bytes_downloaded = 0

    def update_progress(self, progress: float, bytes_downloaded: int = 0) -> None:
        """
        Actualiza el progreso de la descarga.
        
        Args:
            progress (float): Valor del progreso entre 0 y 100
            bytes_downloaded (int): Bytes descargados en este chunk
        """
        self._bytes_downloaded += bytes_downloaded
        current_time = time.time()
        
        if self._start_time is None:
            self._start_time = current_time
        else:
            elapsed_time = current_time - self._start_time
            if elapsed_time > 0:
                self._current_speed = self._bytes_downloaded / elapsed_time
                # Calculate estimated time remaining
                if progress > 0:
                    total_bytes = (self._bytes_downloaded / (progress / 100))
                    remaining_bytes = total_bytes - self._bytes_downloaded
                    estimated_time = remaining_bytes / self._current_speed if self._current_speed > 0 else 0
                else:
                    estimated_time = 0

        self._queue.put(("progress", {
            "progress": progress,
            "speed": self._current_speed,
            "total_downloaded": self._bytes_downloaded,
            "estimated_time": estimated_time if 'estimated_time' in locals() else 0
        }))

    def log_message(self, message: str, error: bool = False) -> None:
        """
        Envía un mensaje al área de log.
        
        Args:
            message (str): Mensaje a mostrar
            error (bool): Indica si el mensaje es de error
        """
        if error:
            self._queue.put(("error", message))
        else:
            self._queue.put(("log", message))

    @property
    def is_cancelled(self) -> bool:
        """
        Verifica si se ha solicitado la cancelación de la descarga.
        
        Returns:
            bool: True si se ha solicitado la cancelación, False en caso contrario
        """
        return self._cancelled

    @property
    def is_paused(self) -> bool:
        """
        Verifica si la descarga está pausada.
        
        Returns:
            bool: True si la descarga está pausada, False en caso contrario
        """
        return self._paused

    def set_cancelled(self, value: bool = True) -> None:
        """
        Establece el estado de cancelación.
        
        Args:
            value (bool): Nuevo estado de cancelación
        """
        self._cancelled = value

    def set_paused(self, value: bool = True) -> None:
        """
        Establece el estado de pausa.
        
        Args:
            value (bool): Nuevo estado de pausa
        """
        self._paused = value

    def reset_stats(self) -> None:
        """
        Reinicia las estadísticas de descarga.
        """
        self._start_time = None
        self._bytes_downloaded = 0
        self._current_speed = 0

    @property
    def current_speed(self) -> float:
        """
        Obtiene la velocidad actual de descarga en bytes por segundo.
        
        Returns:
            float: Velocidad de descarga en bytes por segundo
        """
        return self._current_speed

    def get_estimated_time(self, total_size: int) -> float:
        """
        Calcula el tiempo estimado restante para la descarga.
        
        Args:
            total_size (int): Tamaño total del archivo en bytes
        
        Returns:
            float: Tiempo estimado en segundos, o -1 si no se puede calcular
        """
        if self._current_speed > 0:
            remaining_bytes = total_size - self._bytes_downloaded
            return remaining_bytes / self._current_speed
        return -1