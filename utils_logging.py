import logging
import warnings
import time
from functools import wraps
from typing import Callable, Optional, Any, TypeVar, Union
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.console import Console
from rich.prompt import Confirm

# Suprimir warning de openpyxl
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# GUI Mode flag
_GUI_MODE = False

def set_gui_mode(enabled: bool) -> None:
    """
    Set GUI mode and adjust logging behavior accordingly.
    
    Args:
        enabled: If True, enables GUI mode and suppresses terminal output
    """
    global _GUI_MODE
    _GUI_MODE = enabled
    if enabled:
        logger.setLevel(logging.WARNING)
        console.quiet = True
    else:
        logger.setLevel(logging.INFO)
        console.quiet = False

# Configurar logging con Rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)

logger = logging.getLogger("rich")
console = Console()

class ProgressManager:
    def __init__(self, description: str):
        self.description = description
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        )
        self.task_id = None

    def __enter__(self):
        self.progress.start()
        self.task_id = self.progress.add_task(self.description, total=100)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()

    def update(self, advance: float = 1):
        if self.task_id is not None:
            self.progress.update(self.task_id, advance=advance)

    def set_description(self, description: str):
        if self.task_id is not None:
            self.progress.update(self.task_id, description=description)

F = TypeVar('F', bound=Callable[..., Any])

def with_progress(description: str) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with ProgressManager(description) as progress:
                kwargs['progress_callback'] = progress.update
                result = func(*args, **kwargs)
                return result
        return wrapper
    return decorator

def retry_on_error(
    max_retries: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple = (Exception,)
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries == max_retries:
                        logger.error(f"Error después de {max_retries} intentos: {str(e)}")
                        raise
                    delay = base_delay * (2 ** (retries - 1))  # Exponential backoff
                    logger.warning(f"Intento {retries} fallido. Reintentando en {delay:.1f} segundos...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

def prompt_continue_on_error(error_message: str, error_callback: Optional[Callable[[str], None]] = None) -> bool:
    """
    Muestra un mensaje de error y continúa la ejecución.
    
    Args:
        error_message: Mensaje de error a mostrar
        error_callback: Función callback para mostrar errores en la GUI
    """
    if error_callback:
        error_callback(f"Error: {error_message}")
    return True

def show_file_error(
    file_name: str,
    error_message: str,
    additional_info: Optional[str] = None,
    error_callback: Optional[Callable[[str], None]] = None
) -> None:
    """
    Muestra un mensaje de error formateado relacionado con un archivo.
    
    Args:
        file_name: Nombre del archivo que generó el error
        error_message: Mensaje de error principal
        additional_info: Información adicional opcional
        error_callback: Función callback para mostrar errores en la GUI
    """
    error_text = f"Error procesando archivo {file_name}:\n→ {error_message}"
    if additional_info:
        error_text += f"\nInformación adicional: {additional_info}"
    
    if _GUI_MODE and error_callback:
        error_callback(error_text)
    elif not _GUI_MODE:
        console.print(f"[red]{error_text}[/red]")

def show_progress_message(
    message: str,
    style: str = "info",
    log_callback: Optional[Callable[[str], None]] = None
) -> None:
    """
    Muestra un mensaje de progreso con el estilo especificado.
    
    Args:
        message: Mensaje a mostrar
        style: Estilo del mensaje ('info', 'success', 'warning', 'error')
        log_callback: Función callback para mostrar mensajes en la GUI
    """
    if _GUI_MODE and log_callback:
        log_callback(message)
    elif not _GUI_MODE:
        styles = {
            "info": "blue",
            "success": "green",
            "warning": "yellow",
            "error": "red"
        }
        color = styles.get(style, "white")
        console.print(f"[{color}]{message}[/{color}]")
