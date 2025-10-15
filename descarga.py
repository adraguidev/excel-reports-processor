# descarga.py - Optimización de Descargas

import os
import csv
import chardet
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor
from requests_ntlm import HttpNtlmAuth
import glob
import pandas as pd
from datetime import datetime, timedelta
from file_utils import (
    confirm_overwrite, confirm_overwrite_all, set_global_overwrite_decision,
    analyze_files, display_file_summary, prompt_download_decision
)
from enum import Enum
import msvcrt  # For Windows file locking
import contextlib
import errno

class TimeGrouping(Enum):
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'
    YEARLY = 'yearly'
from utils_logging import (
    logger, with_progress, retry_on_error, show_progress_message
)
from credentials_manager import credentials_manager

# Obtener credenciales desde el manejador local (no desde variables de entorno)
def get_ntlm_credentials():
    """Obtiene las credenciales NTLM desde el archivo local"""
    user, password = credentials_manager.get_credentials()
    if not user or not password:
        logger.warning("No se encontraron credenciales NTLM válidas. Configure las credenciales en la interfaz.")
        return '', ''
    return user.strip(), password

# Control de workers para descargas paralelas - ajustable vía variable de entorno
DEFAULT_WORKERS = 2  # Default to 2 workers if not specified
MAX_WORKERS = int(os.getenv('DOWNLOAD_MAX_WORKERS', DEFAULT_WORKERS))
# Delay entre chunks para throttling de red - ajustable vía variable de entorno (default 0.5s)
DOWNLOAD_DELAY = float(os.getenv('DOWNLOAD_DELAY', 0.5))
# Tamaño de chunk reducido para mejor control de red
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 4096))  # Reduced from 8192 to 4096 bytes
BASE_URL = os.getenv('REPORT_BASE_URL', 'http://172.27.230.27/ReportServer')
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '600'))
REPORT_FORMAT = 'CSV'
STALE_LOCK_MAX_AGE = int(os.getenv('LOCK_MAX_AGE_SECONDS', '0'))  # 0 = eliminar todos los locks pendientes
LOCK_WAIT_TIMEOUT = float(os.getenv('LOCK_WAIT_TIMEOUT', '30'))  # segundos para esperar antes de abortar
LOCK_WAIT_INTERVAL = float(os.getenv('LOCK_WAIT_INTERVAL', '0.5'))  # intervalo entre reintentos
def crear_carpetas():
    """
    Crea y configura las carpetas de salida para cada tipo de trámite.

    Las carpetas se crean utilizando os.makedirs con exist_ok=True,
    lo que significa que no se generará un error si las carpetas ya existen.
    
    Returns:
        dict: Diccionario con tipos de trámite como claves y rutas de carpetas como valores
    """
    tipos_tramite = {58: "CCM", 57: "PRR"}
    output_folders = {}
    for tipo, nombre in tipos_tramite.items():
        folder = os.path.abspath(os.path.join("descargas", nombre))
        os.makedirs(folder, exist_ok=True)
        output_folders[tipo] = folder
    return output_folders


def remove_stale_lock_files(base_dir="descargas", max_age=None, log_callback=None):
    """
    Elimina archivos .lock antiguos que pudieran quedar tras un cierre forzado.

    Args:
        base_dir (str): Directorio base donde buscar archivos .lock.
        max_age (int): Edad máxima en segundos para considerar un lock como obsoleto.
        log_callback (callable): Función para registrar mensajes en la interfaz.

    Returns:
        list[str]: Lista de archivos .lock eliminados.
    """
    if max_age is None:
        max_age = STALE_LOCK_MAX_AGE

    base_path = os.path.abspath(base_dir)
    if not os.path.exists(base_path):
        return []

    removed = []
    now = time.time()

    for root, _, files in os.walk(base_path):
        for name in files:
            if not name.endswith(".lock"):
                continue

            lock_path = os.path.join(root, name)
            try:
                age = now - os.path.getmtime(lock_path)
            except OSError:
                continue

            should_remove = max_age <= 0 or age > max_age

            if should_remove:
                try:
                    os.remove(lock_path)
                    removed.append(lock_path)
                    message = f"Lock limpiado: {lock_path}"
                    if log_callback:
                        log_callback(message)
                    else:
                        logger.info(message)
                except OSError as exc:
                    logger.warning(f"No se pudo eliminar lock obsoleto {lock_path}: {exc}")

    return removed
def generar_urls_por_partes():
    """
    Genera URLs para la descarga de reportes basados en diferentes parámetros.

    Returns:
        dict: Diccionario con tipos de trámite como claves y listas de tuplas (url, año, estado) como valores
    """
    urls_por_partes = {}
    anios = [2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018]
    estados_tramite = ["A", "P", "B", "R", "D", "E", "N"]

    BASE_URL = "http://172.27.230.27/ReportServer"
    REPORT_FORMAT = "CSV"

    def construir_url(tipo, anio, estado):
        return (
            f"{BASE_URL}?%2FAGV_PTP%2FRPT_INMIGRA_PTP_REGUL_CCM"
            f"&nidtipoTramite={tipo}&anio={anio}"
            f"&EstadoTramite={estado}&rs:Format={REPORT_FORMAT}",
            anio, estado
        )

    for tipo in [58, 57, 317, 55]:
        urls_por_partes[tipo] = [construir_url(tipo, anio, estado) 
                                for anio in anios 
                                for estado in estados_tramite]
    return urls_por_partes
def descargar_archivo(url, output_path, progress_callback=None, log_callback=None, error_callback=None, overwrite=False):
    """
    Descarga un archivo usando streaming y manejo de errores mejorado.

    Args:
        url (str): URL del archivo a descargar
        output_path (str): Ruta donde guardar el archivo
        progress_callback (callable): Función para actualizar la barra de progreso
        log_callback (callable): Función para registrar mensajes
        error_callback (callable): Función para manejar errores
        overwrite (bool): Si se debe sobrescribir archivos existentes

    Returns:
        bool: True si la descarga fue exitosa, False en caso contrario
    """
    if os.path.exists(output_path) and not overwrite:
        if log_callback:
            log_callback(f"Omitiendo descarga, archivo existente: {output_path}")
        return True
    
    session = requests.Session()
    session.trust_env = False
    # Obtener credenciales actuales desde el manejador
    current_user, current_pass = get_ntlm_credentials()
    auth = HttpNtlmAuth(current_user, current_pass)
    session.auth = auth
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Connection': 'keep-alive'
    })
    
    retries = 5  # Increased retries
    retry_delay = 5  # Initial delay in seconds
    last_exception = None
    temp_file_path = output_path + '.tmp'
    lock_file_path = output_path + '.lock'
    
    @contextlib.contextmanager
    def file_lock(file_path):
        lock_fd = None
        start_wait = time.time()

        while True:
            try:
                lock_fd = os.open(file_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                break
            except OSError as e:
                if e.errno == errno.EEXIST:
                    try:
                        age = time.time() - os.path.getmtime(file_path)
                    except OSError:
                        age = 0

                    if STALE_LOCK_MAX_AGE <= 0 or age > STALE_LOCK_MAX_AGE:
                        try:
                            os.remove(file_path)
                            message = f"Lock limpiado: {file_path}"
                            if log_callback:
                                log_callback(message)
                            else:
                                logger.info(message)
                            continue
                        except OSError as exc:
                            logger.warning(f"No se pudo eliminar lock obsoleto {file_path}: {exc}")

                    if time.time() - start_wait >= LOCK_WAIT_TIMEOUT:
                        if log_callback:
                            log_callback(f"Otro proceso está descargando el archivo: {output_path}")
                        raise IOError("File is locked by another process")

                    time.sleep(LOCK_WAIT_INTERVAL)
                    continue
                raise

        try:
            msvcrt.locking(lock_fd, msvcrt.LK_NBLCK, 1)
            yield
        finally:
            try:
                msvcrt.locking(lock_fd, msvcrt.LK_UNLCK, 1)
            except (IOError, OSError):
                pass
            if lock_fd is not None:
                os.close(lock_fd)
            try:
                os.remove(file_path)
            except (IOError, OSError):
                pass
    
    for attempt in range(retries):
        try:
            if log_callback:
                log_callback(f"Intento {attempt + 1} de {retries} - Iniciando descarga: {url}")
            
            # Get direct download preference from environment
            direct_download = os.getenv('DIRECT_DOWNLOAD', 'false').lower() == 'true'
            
            # Reset session for each attempt to avoid stale connections
            if attempt > 0:
                session.close()
                session = requests.Session()
                session.trust_env = False
                # Obtener credenciales actualizadas para cada intento
                current_user, current_pass = get_ntlm_credentials()
                auth = HttpNtlmAuth(current_user, current_pass)
                session.auth = auth
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Connection': 'keep-alive'
                })
            
            # Try to acquire the lock before proceeding with download
            with file_lock(lock_file_path):
                response = session.get(
                    url,
                    timeout=REQUEST_TIMEOUT,
                    stream=not direct_download,
                    auth=auth
                )
                if response.status_code == 401:
                    response.close()
                    raise PermissionError("Autenticación NTLM rechazada (401 Unauthorized)")
                response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            bytes_downloaded = 0
            last_progress_update = time.time()
            
            # Create a new temp file for each attempt
            with open(temp_file_path, 'wb') as file:
                if direct_download:
                    # Direct download without chunks
                    content = response.content
                    file.write(content)
                    if progress_callback and total_size > 0:
                        progress_callback(100)
                else:
                    # Chunk-based download with progress tracking
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            file.write(chunk)
                            bytes_downloaded += len(chunk)
                            
                            # Update progress less frequently to reduce overhead
                            current_time = time.time()
                            if current_time - last_progress_update >= 0.5:  # Update every 500ms
                                if progress_callback and total_size > 0:
                                    progress = (bytes_downloaded / total_size) * 100
                                    progress_callback(progress)
                                last_progress_update = current_time
                                
                            if DOWNLOAD_DELAY > 0:
                                time.sleep(DOWNLOAD_DELAY)
            
            # Verify file size if content-length was provided
            if total_size > 0 and os.path.getsize(temp_file_path) != total_size:
                raise requests.exceptions.RequestException("Downloaded file size mismatch")
            
            # Only rename the file if download completed successfully
            try:
                os.replace(temp_file_path, output_path)
                if log_callback:
                    log_callback(f"Archivo descargado correctamente: {output_path}")
                return True
            except OSError as e:
                if e.errno == errno.EACCES:
                    # File is being used by another process
                    if error_callback:
                        error_callback(output_path, "El archivo está siendo usado por otro proceso")
                    time.sleep(1)  # Wait before retry
                    continue
                raise
    
        except PermissionError as e:
            last_exception = e
            if error_callback:
                error_callback(output_path, str(e))
            break
        
        except requests.exceptions.RequestException as e:
            last_exception = e
            if error_callback:
                error_callback(output_path, f"Error de conexión (intento {attempt + 1}): {str(e)}")
            
            # Clean up temp file if it exists
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
            
            if attempt < retries - 1:
                # Exponential backoff with jitter
                jitter = random.uniform(0, 1)
                sleep_time = (retry_delay * (2 ** attempt)) + jitter
                
                # Extra delay for server errors (503, 502, 504)
                if "503" in str(e) or "502" in str(e) or "504" in str(e):
                    sleep_time += random.uniform(5, 15)  # Extra 5-15 seconds for server errors
                    if log_callback:
                        log_callback(f"Error de servidor detectado, esperando {sleep_time:.1f}s antes del reintento...")
                
                time.sleep(sleep_time)
            continue
            
        except Exception as e:
            if error_callback:
                error_callback(output_path, f"Error inesperado: {str(e)}")
            # Clean up temp file if it exists
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
            return False
            
        finally:
            session.close()
    
    # If we get here, all retries failed
    if error_callback and last_exception:
        error_callback(output_path, f"Descarga fallida después de {retries} intentos. Último error: {str(last_exception)}")
    return False
def descargar_en_paralelo(tipo, urls, output_folder, progress_callback=None, log_callback=None, error_callback=None):
    """
    Gestiona las descargas en paralelo con mejor manejo de errores.
    
    Args:
        tipo (int): Tipo de trámite
        urls (list): Lista de tuplas (url, año, estado)
        output_folder (str): Carpeta de destino
        progress_callback (callable): Función para actualizar la barra de progreso
        log_callback (callable): Función para registrar mensajes
        error_callback (callable): Función para manejar errores

    Returns:
        list: Lista de rutas de archivos descargados exitosamente
    """
    total_files = len(urls)
    completed_files = 0
    downloaded_files = []
    tasks = []
    
    # Get current worker count from environment
    current_workers = int(os.getenv('DOWNLOAD_MAX_WORKERS', DEFAULT_WORKERS))
    with ThreadPoolExecutor(max_workers=current_workers) as executor:
        # Create tasks list with futures and their corresponding file paths
        for url, anio, estado in urls:
            file_path = os.path.join(output_folder, f"{anio}_{estado}.csv")
            future = executor.submit(
                descargar_archivo, url, file_path,
                progress_callback=None, log_callback=log_callback,
                error_callback=error_callback, overwrite=True
            )
            tasks.append((future, file_path))
        
        # Process completed tasks
        for future, file_path in tasks:
            try:
                if future.result():
                    downloaded_files.append(file_path)
                completed_files += 1
                if progress_callback:
                    progress = (completed_files / total_files) * 80  # Scale to 80%
                    progress_callback(progress)
            except requests.exceptions.RequestException as e:
                if error_callback:
                    error_callback("archivo", f"Error de conexión en la descarga: {str(e)}")
                continue
            except Exception as e:
                if error_callback:
                    error_callback("archivo", f"Error inesperado en la descarga: {str(e)}")
                continue
    
    return downloaded_files
def descargar_y_consolidar(download_option='all', progress_callback=None, log_callback=None, error_callback=None, overwrite=False, selected_modules=None):
    """
    Gestiona el proceso de descarga y consolidación de archivos.

    Args:
        download_option (str): 'all' para descargar todos, 'missing' para faltantes, 'consolidate' para solo consolidar
        progress_callback (callable): Función para actualizar el progreso
        log_callback (callable): Función para registrar mensajes
        error_callback (callable): Función para manejar errores
        overwrite (bool): Si se debe sobrescribir archivos existentes
    """
    folders = crear_carpetas()
    remove_stale_lock_files(log_callback=log_callback)
    
    # Create a mapping of module names to IDs
    module_name_to_id = {"CCM": 58, "PRR": 57}
    id_to_name = {v: k for k, v in module_name_to_id.items()}
    
    # Filter folders based on selected modules
    if selected_modules:
        # Convert selected module names to IDs
        selected_ids = {module_name_to_id[name]: True for name, selected in selected_modules.items() if selected}
        folders = {k: v for k, v in folders.items() if selected_ids.get(k, False)}
        if not folders:  # If no modules are selected
            if log_callback:
                log_callback("No se han seleccionado módulos para procesar.")
            return
        if log_callback:
            # Convert IDs back to names for logging
            selected_names = [id_to_name[id] for id in folders.keys()]
            log_callback(f"Módulos seleccionados: {', '.join(selected_names)}")
    
    # Early branch for consolidate-only mode
    if download_option == 'consolidate':
        if log_callback:
            log_callback("Iniciando proceso de consolidación...")
        
        consolidation_success = True
        # Process each folder for consolidation
        for tipo, folder in folders.items():
            folder_name = os.path.basename(folder)
            output_file = os.path.join(folder, f"consolidado_total_{folder_name}.csv")
            
            if log_callback:
                log_callback(f"Consolidando archivos de {folder_name}...")
            
            # Get all CSV files except previously consolidated ones
            all_files = glob.glob(os.path.join(folder, "*.csv"))
            files_to_consolidate = [f for f in all_files if 'consolidado_total' not in f]
            
            # Consolidate with full progress range (0-100%)
            try:
                consolidar_archivos_descargados(
                    folder, output_file, files_to_consolidate,
                    progress_callback, log_callback, error_callback,
                    base_progress=0, progress_weight=100
                )
            except Exception as e:
                if error_callback:
                    error_callback(f"consolidacion_{folder_name}", f"Error en consolidación: {str(e)}")
                consolidation_success = False
        
        if log_callback:
            if consolidation_success:
                log_callback("Proceso de consolidación completado exitosamente.")
            else:
                log_callback("Proceso de consolidación completado con algunos errores.")
        
        return consolidation_success

    # Regular download and consolidation logic
    urls_por_partes = generar_urls_por_partes()
    
    # Filter URLs based on selected modules
    if selected_modules:
        urls_por_partes = {k: v for k, v in urls_por_partes.items() if k in [module_name_to_id[name] for name, selected in selected_modules.items() if selected]}
    downloaded_by_type = {}
    
    # Analyze existing files for 'missing' mode
    if download_option == 'missing':
        analysis = analyze_files(
            {str(k): [(url, f"{anio}_{estado}.csv") for url, anio, estado in v] 
             for k, v in urls_por_partes.items()}, 
            {str(k): v for k, v in folders.items()}
        )
    
    if log_callback:
        log_callback("Iniciando proceso de descarga...")
    
    # Process downloads
    for tipo, urls in urls_por_partes.items():
        if log_callback:
            log_callback(f"Procesando descargas para tipo {tipo}...")
        output_folder = folders[tipo]
        
        if download_option == "missing":
            urls_to_download = []
            for url, anio, estado in urls:
                file_path = os.path.join(output_folder, f"{anio}_{estado}.csv")
                if not os.path.exists(file_path):
                    urls_to_download.append((url, anio, estado))
            
            if urls_to_download:
                if log_callback:
                    log_callback(f"Descargando {len(urls_to_download)} archivos faltantes...")
                downloaded_by_type[tipo] = descargar_en_paralelo(
                    tipo, urls_to_download, output_folder,
                    progress_callback, log_callback, error_callback
                )
            else:
                if log_callback:
                    log_callback("No hay archivos faltantes para descargar.")
                downloaded_by_type[tipo] = []
        else:  # download_option == "all"
            if log_callback:
                log_callback(f"Descargando/actualizando todos los archivos...")
            downloaded_by_type[tipo] = descargar_en_paralelo(
                tipo, urls, output_folder,
                progress_callback, log_callback, error_callback
            )
    
    if log_callback:
        log_callback("Proceso de descarga completado.")
        log_callback("Iniciando proceso de consolidación...")
    
    # Consolidate downloaded files
    consolidation_success = True
    for tipo, folder in folders.items():
        folder_name = os.path.basename(folder)
        output_file = os.path.join(folder, f"consolidado_total_{folder_name}.csv")
        
        if log_callback:
            log_callback(f"Consolidando archivos de {folder_name}...")
        
        try:
            consolidar_archivos_descargados(
                folder, output_file, downloaded_by_type.get(tipo, []),
                progress_callback, log_callback, error_callback,
                base_progress=80, progress_weight=20
            )
        except Exception as e:
            if error_callback:
                error_callback(f"consolidacion_{folder_name}", f"Error en consolidación: {str(e)}")
            consolidation_success = False
    
    if log_callback:
        if consolidation_success:
            log_callback("Proceso de descarga y consolidación completado exitosamente.")
        else:
            log_callback("Proceso completado con algunos errores en la consolidación.")
    
    return consolidation_success

class OperatorReport:
    """Clase para generar reportes de operadores basados en los datos consolidados."""
    
    def _map_and_validate_columns(self):
        """
        Maps alternate column names and validates required columns existence.
        Includes historical column mappings and fallback mechanisms.
        
        Raises:
            ValueError: If critical required columns are missing after mapping attempt.
        """
        required_columns = [
            'FechaPre',
            'OperadorPre',
            'NumeroTramite',
            'FechaExpendiente'
        ]
        
        optional_columns = [
            'FechaEtapaAprobacionMasivaFin'
        ]
        
        # Extended column mapping dictionary with historical names
        column_mappings = {
            'FECHA DE TRABAJO': 'FechaPre',
            'FECHA TRABAJO': 'FechaPre',
            'FECHA_TRABAJO': 'FechaPre',
            'EVALUADOR': 'OperadorPre',
            'OPERADOR': 'OperadorPre',
            'EVALUADOR_PRE': 'OperadorPre',
            'EXPEDIENTE': 'NumeroTramite',
            'NRO_EXPEDIENTE': 'NumeroTramite',
            'NRO_TRAMITE': 'NumeroTramite',
            'FECHA_APROBACION': 'FechaEtapaAprobacionMasivaFin',
            'FECHA_EXPEDIENTE': 'FechaExpendiente'
        }
        
        # Apply column mappings
        for old_col, new_col in column_mappings.items():
            if old_col in self.df.columns and new_col not in self.df.columns:
                self.df.rename(columns={old_col: new_col}, inplace=True)
        
        # Handle special date columns
        if 'FechaEtapaAprobacionMasivaFin' not in self.df.columns:
            self.df['FechaEtapaAprobacionMasivaFin'] = pd.NaT
            
        if 'FechaExpendiente' not in self.df.columns:
            # Use FechaPre as FechaExpendiente if available, otherwise NaT
            self.df['FechaExpendiente'] = self.df['FechaPre'] if 'FechaPre' in self.df.columns else pd.NaT
        
        # Check for missing columns after mapping
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            raise ValueError(
                f"Las siguientes columnas requeridas están ausentes en el archivo: {', '.join(missing_columns)}"
            )
    
    def _standardize_dates(self):
        """
        Standardizes date columns format using consistent parsing.
        Attempts multiple date formats for better compatibility with historical data.
        """
        date_columns = ['FechaPre', 'FechaEtapaAprobacionMasivaFin', 'FechaExpendiente']
        date_formats = [
            '%d/%m/%Y %H:%M',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d'
        ]
        
        for col in date_columns:
            if col in self.df.columns:
                # Try each format until one works
                for date_format in date_formats:
                    try:
                        self.df[col] = pd.to_datetime(
                            self.df[col],
                            format=date_format,
                            dayfirst=True,
                            errors='coerce'
                        )
                        # If successful, break the format loop
                        if self.df[col].notna().any():
                            break
                    except (ValueError, TypeError):
                        continue
                
                # If no format worked, try the generic parser as last resort
                if self.df[col].isna().all():
                    self.df[col] = pd.to_datetime(
                        self.df[col],
                        dayfirst=True,
                        errors='coerce'
                    )
    
    def __init__(self, consolidated_file_path):
        """
        Inicializa el generador de reportes.
        
        Args:
            consolidated_file_path (str): Ruta al archivo CSV consolidado
            
        Raises:
            ValueError: Si faltan columnas requeridas en el archivo
        """
        self.df = pd.read_csv(consolidated_file_path, encoding='utf-8-sig', low_memory=False)
        
        # Map and validate columns
        self._map_and_validate_columns()
        
        # Standardize date columns
        self._standardize_dates()
        
        # Create helper columns for completion status and date with historical compatibility
        self.df['is_completed'] = ~(
            self.df['FechaPre'].isna() & 
            self.df['FechaEtapaAprobacionMasivaFin'].isna()
        )
        
        # Create completion date with fallback mechanism
        completion_columns = ['FechaPre', 'FechaEtapaAprobacionMasivaFin']
        self.df['completion_date'] = self.df[completion_columns].min(axis=1)
        
        # Additional helper columns for historical analysis
        self.df['processing_time'] = (
            self.df['FechaEtapaAprobacionMasivaFin'] - self.df['FechaPre']
        ).dt.total_seconds() / 86400  # Convert to days
        
        # Mark active status
        self.df['is_active'] = ~self.df['FechaPre'].isna()
    
    def get_workload_metrics(self, start_date=None, end_date=None, grouping=TimeGrouping.MONTHLY):
        """
        Genera métricas de carga de trabajo, completados y backlog por período.
        
        Args:
            start_date (str): Fecha inicial en formato 'dd/mm/YYYY'
            end_date (str): Fecha final en formato 'dd/mm/YYYY'
            grouping (TimeGrouping): Agrupación temporal (daily, weekly, monthly, yearly)
            
        Returns:
            pd.DataFrame: DataFrame con métricas por período incluyendo:
                - incoming: Total de expedientes ingresados
                - completions: Total de expedientes completados
                - active_evaluators: Número de evaluadores únicos
                - period_backlog: Backlog del período
                - cumulative_backlog: Backlog acumulado
                - completion_rate: Tasa de completitud
                - avg_processing_time: Tiempo promedio de procesamiento
                - productivity_per_evaluator: Productividad por evaluador
                - days_to_resolve_backlog: Días estimados para resolver el backlog
        """
        # Filtrar por rango de fechas si se especifica
        df_filtered = self.df.copy()
        
        if start_date:
            start_dt = datetime.strptime(start_date, '%d/%m/%Y').date()
            df_filtered = df_filtered[df_filtered['FechaExpendiente'].dt.date >= start_dt]
        if end_date:
            end_dt = datetime.strptime(end_date, '%d/%m/%Y').date()
            df_filtered = df_filtered[df_filtered['FechaExpendiente'].dt.date <= end_dt]
            
        # Crear columna de período según la agrupación especificada
        if grouping == TimeGrouping.DAILY:
            df_filtered['Periodo'] = df_filtered['FechaExpendiente'].dt.date
        elif grouping == TimeGrouping.WEEKLY:
            df_filtered['Periodo'] = df_filtered['FechaExpendiente'].dt.strftime('%Y-W%U')
        elif grouping == TimeGrouping.MONTHLY:
            df_filtered['Periodo'] = df_filtered['FechaExpendiente'].dt.strftime('%Y-%m')
        else:  # YEARLY
            df_filtered['Periodo'] = df_filtered['FechaExpendiente'].dt.year
        
        # Calcular métricas básicas por período
        metrics = df_filtered.groupby('Periodo').agg({
            'NumeroTramite': 'count',  # incoming
            'is_completed': 'sum',     # completions
            'OperadorPre': 'nunique',  # active_evaluators
            'processing_time': 'mean'   # avg_processing_time
        }).reset_index()
        
        # Renombrar columnas
        metrics.columns = ['Periodo', 'incoming', 'completions', 'active_evaluators', 'avg_processing_time']
        
        # Calcular métricas adicionales
        metrics['period_backlog'] = metrics['incoming'] - metrics['completions']
        metrics['cumulative_backlog'] = metrics['period_backlog'].cumsum()
        
        # Calcular tasa de completitud (con manejo de división por cero)
        metrics['completion_rate'] = metrics.apply(
            lambda x: x['completions'] / x['incoming'] if x['incoming'] > 0 else 0,
            axis=1
        )
        
        # Calcular productividad por evaluador (con manejo de división por cero)
        metrics['productivity_per_evaluator'] = metrics.apply(
            lambda x: x['completions'] / x['active_evaluators'] if x['active_evaluators'] > 0 else 0,
            axis=1
        )
        
        # Calcular días estimados para resolver backlog (con manejo de división por cero)
        metrics['days_to_resolve_backlog'] = metrics.apply(
            lambda x: x['cumulative_backlog'] / (x['completions'] / 30) if x['completions'] > 0 else float('inf'),
            axis=1
        )
        
        # Ordenar por período
        metrics = metrics.sort_values('Periodo')
        
        return metrics

    def get_operator_workload(self, start_date=None, end_date=None, grouping=TimeGrouping.MONTHLY):
        """
        Genera un reporte de carga de trabajo por operador en formato de tabla pivote.
        
        Args:
            start_date (str): Fecha inicial en formato 'dd/mm/YYYY'
            end_date (str): Fecha final en formato 'dd/mm/YYYY'
            grouping (TimeGrouping): Agrupación temporal (daily, weekly, monthly, yearly)
            
        Returns:
            pd.DataFrame: DataFrame con el reporte de carga de trabajo en formato pivot,
                        donde las filas son operadores y las columnas son períodos
        """
        # Filtrar por rango de fechas si se especifica
        df_filtered = self.df.copy()
        
        # Eliminar filas donde FechaPre es nulo
        df_filtered = df_filtered.dropna(subset=['FechaPre'])
        
        # Convertir fechas de entrada y filtrar
        if start_date:
            start_dt = datetime.strptime(start_date, '%d/%m/%Y').date()
            df_filtered = df_filtered[df_filtered['FechaPre'].dt.date >= start_dt]
        if end_date:
            end_dt = datetime.strptime(end_date, '%d/%m/%Y').date()
            df_filtered = df_filtered[df_filtered['FechaPre'].dt.date <= end_dt]
            
        # Crear columnas de agrupación según el período especificado
        if grouping == TimeGrouping.DAILY:
            df_filtered['Periodo'] = df_filtered['FechaPre'].dt.date
        elif grouping == TimeGrouping.WEEKLY:
            df_filtered['Periodo'] = df_filtered['FechaPre'].dt.strftime('%Y-W%U')
        elif grouping == TimeGrouping.MONTHLY:
            df_filtered['Periodo'] = df_filtered['FechaPre'].dt.strftime('%Y-%m')
        else:  # YEARLY
            df_filtered['Periodo'] = df_filtered['FechaPre'].dt.year
            
        # Generar el reporte usando pivot_table
        pivot_report = pd.pivot_table(
            df_filtered,
            index='OperadorPre',
            columns='Periodo',
            values='NumeroTramite',
            aggfunc='count',
            fill_value=0
        )
        
        # Renombrar el índice para mostrar "Evaluadores"
        pivot_report.index.name = 'Evaluadores'
        
        # Agregar columna de total
        pivot_report['Total'] = pivot_report.sum(axis=1)
        
        # Ordenar las columnas cronológicamente (excepto la columna Total que debe quedar al final)
        period_columns = sorted([col for col in pivot_report.columns if col != 'Total'])
        pivot_report = pivot_report[period_columns + ['Total']]
        
        return pivot_report
    
    def get_operator_summary(self, start_date=None, end_date=None):
        """
        Genera un resumen general de la carga de trabajo por operador.
        
        Args:
            start_date (str): Fecha inicial en formato 'YYYY-MM-DD'
            end_date (str): Fecha final en formato 'YYYY-MM-DD'
            
        Returns:
            pd.DataFrame: DataFrame con el resumen de trabajo por operador
        """
        df_filtered = self.df.copy()
        if start_date:
            df_filtered = df_filtered[df_filtered['FechaPre'] >= start_date]
        if end_date:
            df_filtered = df_filtered[df_filtered['FechaPre'] <= end_date]
            
        summary = df_filtered.groupby('OperadorPre').agg({
            'NumeroTramite': 'count',
            'FechaPre': ['min', 'max']
        }).reset_index()
        
        summary.columns = ['Operador', 'Total_Tramites', 'Primera_Actividad', 'Ultima_Actividad']
        return summary
    
    def export_report(self, report_df, output_path, format='csv'):
        """
        Exporta el reporte al formato especificado.
        
        Args:
            report_df (pd.DataFrame): DataFrame con el reporte
            output_path (str): Ruta de salida para el archivo
            format (str): Formato de salida ('csv' o 'excel')
        """
        if format.lower() == 'csv':
            report_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        elif format.lower() == 'excel':
            report_df.to_excel(output_path, index=False)

    def analyze_evaluator_variation(self, start_date=None, end_date=None, grouping=TimeGrouping.MONTHLY):
        """
        Analiza la variación en la productividad de los evaluadores a lo largo del tiempo.

        Args:
            start_date (str): Fecha inicial en formato 'dd/mm/YYYY'
            end_date (str): Fecha final en formato 'dd/mm/YYYY'
            grouping (TimeGrouping): Agrupación temporal (daily, weekly, monthly, yearly)

        Returns:
            dict: Diccionario con los siguientes elementos:
                - workload_metrics: DataFrame con métricas detalladas de carga de trabajo
                - variations: Dict con las variaciones entre períodos consecutivos
                - projections: Dict con proyecciones detalladas incluyendo necesidades de personal
        """

    def get_pending_cases(self, start_date=None, end_date=None, grouping=TimeGrouping.MONTHLY):
        """
        Filtra y agrupa los casos pendientes por período.
        Un caso se considera pendiente si falta FechaExpendiente o FechaEtapaAprobacionMasivaFin.

        Args:
            start_date (str): Fecha inicial en formato 'dd/mm/YYYY'
            end_date (str): Fecha final en formato 'dd/mm/YYYY'
            grouping (TimeGrouping): Agrupación temporal (daily, weekly, monthly, yearly)

        Returns:
            pd.DataFrame: DataFrame con el conteo de casos pendientes por período, incluyendo:
                - total_pending: Total de casos pendientes
                - pending_no_expediente: Casos sin FechaExpendiente
                - pending_no_aprobacion: Casos sin FechaEtapaAprobacionMasivaFin
                - avg_waiting_days: Promedio de días en espera

        Raises:
            ValueError: Si faltan columnas requeridas o si hay error en el procesamiento
        """
        try:
            # 1. Validar columnas requeridas
            required_columns = ['FechaPre', 'FechaExpendiente', 'FechaEtapaAprobacionMasivaFin', 'NumeroTramite']
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            if missing_columns:
                raise ValueError(f"Columnas requeridas faltantes: {', '.join(missing_columns)}")

            # 2. Convertir columnas de fecha a datetime
            date_columns = ['FechaPre', 'FechaExpendiente', 'FechaEtapaAprobacionMasivaFin']
            df_processed = self.df.copy()
            for col in date_columns:
                df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')

            # 3. Filtrar casos pendientes
            pending_df = df_processed[
                df_processed['FechaExpendiente'].isna() | 
                df_processed['FechaEtapaAprobacionMasivaFin'].isna()
            ].copy()

            # 4. Aplicar filtros de fecha si se especifican
            if start_date:
                start_dt = pd.to_datetime(start_date, format='%d/%m/%Y')
                pending_df = pending_df[pending_df['FechaPre'] >= start_dt]
            if end_date:
                end_dt = pd.to_datetime(end_date, format='%d/%m/%Y')
                pending_df = pending_df[pending_df['FechaPre'] <= end_dt]

            # 5. Calcular días en espera
            current_date = pd.Timestamp.now()
            pending_df['waiting_days'] = (current_date - pending_df['FechaPre']).dt.total_seconds() / (24 * 3600)

            # 6. Definir la frecuencia de agrupación
            freq_map = {
                TimeGrouping.DAILY: 'D',
                TimeGrouping.WEEKLY: 'W',
                TimeGrouping.MONTHLY: 'M',
                TimeGrouping.YEARLY: 'Y'
            }
            freq = freq_map[grouping]

            # 7. Agrupar y calcular métricas
            grouped = pending_df.groupby(pd.Grouper(key='FechaPre', freq=freq)).agg({
                'NumeroTramite': 'count',  # total_pending
                'FechaExpendiente': lambda x: x.isna().sum(),  # pending_no_expediente
                'FechaEtapaAprobacionMasivaFin': lambda x: x.isna().sum(),  # pending_no_aprobacion
                'waiting_days': 'mean'  # avg_waiting_days
            }).reset_index()

            # 8. Renombrar columnas
            grouped.columns = [
                'Periodo',
                'total_pending',
                'pending_no_expediente',
                'pending_no_aprobacion',
                'avg_waiting_days'
            ]

            # 9. Redondear días en espera y filtrar grupos sin pendientes
            grouped['avg_waiting_days'] = grouped['avg_waiting_days'].round(1)
            grouped = grouped[grouped['total_pending'] > 0]
            grouped = grouped.sort_values('Periodo')

            return grouped

        except Exception as e:
            raise ValueError(f"Error al procesar casos pendientes: {str(e)}")
        # Obtener métricas de carga de trabajo
        workload_metrics = self.get_workload_metrics(start_date, end_date, grouping)
        
        # Calcular productividad promedio por período
        avg_productivity = workload_metrics['productivity_per_evaluator'].mean()
        
        # Calcular variaciones entre períodos consecutivos
        variations = {
            'evaluators': workload_metrics['active_evaluators'].diff().fillna(0),
            'production': workload_metrics['completions'].diff().fillna(0),
            'productivity': workload_metrics['productivity_per_evaluator'].diff().fillna(0),
            'backlog': workload_metrics['cumulative_backlog'].diff().fillna(0)
        }
        
        # Calcular proyecciones avanzadas
        projections = {}
        for _, row in workload_metrics.iterrows():
            period = row['Periodo']
            current_backlog = row['cumulative_backlog']
            current_incoming = row['incoming']
            current_evaluators = row['active_evaluators']
            current_productivity = row['productivity_per_evaluator']
            
            # Calcular evaluadores necesarios para diferentes aspectos
            if avg_productivity > 0:
                evaluators_for_backlog = current_backlog / (avg_productivity * 30)  # 30 días por mes
                evaluators_for_incoming = current_incoming / avg_productivity
                total_evaluators_needed = evaluators_for_backlog + evaluators_for_incoming
                
                # Calcular tiempo estimado para resolver backlog con capacidad actual
                if current_productivity > 0 and current_evaluators > 0:
                    estimated_months = current_backlog / (current_productivity * current_evaluators * 30)
                else:
                    estimated_months = float('inf')
                
                projections[period] = {
                    'current_evaluators': current_evaluators,
                    'current_productivity': current_productivity,
                    'evaluators_for_backlog': round(evaluators_for_backlog, 2),
                    'evaluators_for_incoming': round(evaluators_for_incoming, 2),
                    'evaluators_needed_total': round(total_evaluators_needed, 2),
                    'estimated_months_to_resolve': round(estimated_months, 1),
                    'backlog': current_backlog,
                    'potential_production': round(current_evaluators * avg_productivity, 2),
                    'production_gap': round(current_evaluators * (avg_productivity - current_productivity), 2)
                }
            else:
                # Manejar caso donde no hay productividad promedio
                projections[period] = {
                    'current_evaluators': current_evaluators,
                    'current_productivity': current_productivity,
                    'evaluators_for_backlog': float('inf'),
                    'evaluators_for_incoming': float('inf'),
                    'evaluators_needed_total': float('inf'),
                    'estimated_months_to_resolve': float('inf'),
                    'backlog': current_backlog,
                    'potential_production': 0,
                    'production_gap': 0
                }
        
        return {
            'workload_metrics': workload_metrics,
            'variations': variations,
            'projections': projections,
            'avg_productivity': avg_productivity
        }

def consolidar_archivos_descargados(folder, output_file, downloaded_files, progress_callback=None, log_callback=None, error_callback=None, base_progress=80, progress_weight=20):
    """Consolida los archivos CSV descargados, tomando la fila 4 como encabezado.

    Args:
        folder (str): Ruta de la carpeta con los archivos CSV.
        output_file (str): Ruta del archivo consolidado de salida.
        downloaded_files (list): Lista de rutas de archivos descargados a consolidar.
        progress_callback (callable): Función para actualizar el progreso de consolidación.
        log_callback (callable): Función para registrar mensajes.
        error_callback (callable): Función para manejar errores.
        base_progress (int): Progreso base desde donde comenzar (default 80).
        progress_weight (int): Peso del progreso de consolidación (default 20).
    """
    if not downloaded_files:
        if log_callback:
            log_callback(f"No hay archivos nuevos para consolidar en {folder}")
        return

    dataframes = []
    total_files = len(downloaded_files)
    
    for index, archivo in enumerate(downloaded_files):
        try:
            # Leer CSV con UTF-8-SIG para manejar BOM y caracteres especiales
            df = pd.read_csv(archivo, skiprows=3, encoding='utf-8-sig', on_bad_lines='skip', dtype=str)
            if log_callback:
                log_callback(f"Archivo {os.path.basename(archivo)} leído correctamente")
            
            # Filtrar solo registros que comienzan con 'LM'
            total_records = len(df)
            df = df[df['NumeroTramite'].str.startswith('LM')]
            if log_callback:
                log_callback(f"Records with 'LM' in {os.path.basename(archivo)}: {len(df)} out of {total_records}")
            
            # Agregar columna con el nombre del archivo origen
            df['ARCHIVO_ORIGEN'] = os.path.basename(archivo)
            dataframes.append(df)
            if progress_callback:
                consolidation_progress = base_progress + ((index + 1) / total_files * progress_weight)
                progress_callback(consolidation_progress)
        except Exception as e:
            if error_callback:
                error_callback(archivo, f"Error al procesar archivo: {str(e)}")
            continue

    if not dataframes:
        if log_callback:
            log_callback("No se pudo procesar ningún archivo correctamente")
        return

    try:
        df_consolidado = pd.concat(dataframes, ignore_index=True)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df_consolidado.to_csv(output_file, 
                            index=False,
                            encoding='utf-8-sig',
                            quoting=csv.QUOTE_NONNUMERIC,
                            escapechar='\\',
                            sep=',')
        if log_callback:
            log_callback(f"Consolidado guardado en: {output_file}")
            log_callback(f"Total de registros consolidados: {len(df_consolidado):,}")
    except Exception as e:
        if error_callback:
            error_callback(output_file, f"Error al guardar el consolidado: {str(e)}")
