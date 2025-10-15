import os
from typing import List, Dict, Tuple
from rich.console import Console
from rich.table import Table

# Variable global para almacenar la decisión de sobrescritura
_global_overwrite_decision = None

def set_global_overwrite_decision(decision: bool):
    """
    Establece la decisión global de sobrescritura.
    
    Args:
        decision (bool): True para sobrescribir todos los archivos, False para no sobrescribir
    """
    global _global_overwrite_decision
    _global_overwrite_decision = decision

def confirm_overwrite_all(existing_files: List[str]) -> bool:
    """
    Verifica si hay archivos existentes y retorna la decisión global de sobrescritura.
    
    Args:
        existing_files (List[str]): Lista de rutas de archivos existentes
        
    Returns:
        bool: True si se autoriza la sobrescritura de todos los archivos, False si no
    """
    if not existing_files:
        return True
    
    global _global_overwrite_decision
    if _global_overwrite_decision is None:
        _global_overwrite_decision = False
    
    return _global_overwrite_decision

def analyze_files(urls_por_partes: Dict[str, List[Tuple[str, str]]], folders: Dict[str, str]) -> Dict[str, Dict[str, List[str]]]:
    """
    Analiza los archivos existentes y faltantes por módulo.
    
    Args:
        urls_por_partes (Dict[str, List[Tuple[str, str]]]): Diccionario de URLs por módulo
        folders (Dict[str, str]): Diccionario de carpetas destino por módulo
        
    Returns:
        Dict[str, Dict[str, List[str]]]: Diccionario con archivos existentes y faltantes por módulo
    """
    analysis = {}
    
    for modulo, urls in urls_por_partes.items():
        folder = folders.get(modulo, '')
        existing_files = []
        missing_files = []
        
        for _, filename in urls:
            file_path = os.path.join(folder, filename)
            if os.path.exists(file_path):
                existing_files.append(file_path)
            else:
                missing_files.append(file_path)
        
        analysis[modulo] = {
            'existing': existing_files,
            'missing': missing_files
        }
    
    return analysis

def display_file_summary(analysis: Dict[str, Dict[str, List[str]]], log_callback=None):
    """
    Muestra un resumen de los archivos existentes y faltantes.
    
    Args:
        analysis (Dict[str, Dict[str, List[str]]]): Resultado del análisis de archivos
        log_callback (callable, optional): Función callback para registrar mensajes
    """
    def log_line(message):
        if log_callback:
            log_callback(message)
        
    # Encabezado
    header = f"{'Módulo':<20} {'Existentes':>12} {'Faltantes':>12} {'Total':>12}"
    separator = "-" * len(header)
    
    log_line("\nResumen de archivos:")
    log_line(separator)
    log_line(header)
    log_line(separator)
    
    total_existing = 0
    total_missing = 0
    
    # Datos por módulo
    for modulo, data in analysis.items():
        existing = len(data['existing'])
        missing = len(data['missing'])
        total = existing + missing
        total_existing += existing
        total_missing += missing
        
        log_line(f"{modulo:<20} {existing:>12} {missing:>12} {total:>12}")
    
    # Totales
    log_line(separator)
    log_line(f"{'TOTAL':<20} {total_existing:>12} {total_missing:>12} {total_existing + total_missing:>12}")
    log_line(separator)

def prompt_download_decision(analysis: Dict[str, Dict[str, List[str]]], download_option: str = "missing") -> str:
    """
    Retorna la decisión de descarga basada en el parámetro proporcionado por la GUI.
    
    Args:
        analysis (Dict[str, Dict[str, List[str]]]): Resultado del análisis de archivos
        download_option (str): Opción de descarga ('all', 'missing', 'cancel')
        
    Returns:
        str: Decisión de descarga ('all', 'missing', 'cancel')
    """
    if download_option not in ['all', 'missing', 'cancel']:
        return 'cancel'
    
    set_global_overwrite_decision(download_option == 'all')
    return download_option

def confirm_overwrite(file_path: str, force: bool = False) -> bool:
    """
    Verifica si un archivo existe y retorna la decisión de sobrescritura basada en la configuración global.
    
    Args:
        file_path (str): Ruta del archivo a verificar
        force (bool): Si es True, sobrescribe sin preguntar
        
    Returns:
        bool: True si se puede escribir/sobrescribir, False si no
    """
    if not os.path.exists(file_path) or force:
        return True
        
    global _global_overwrite_decision
    if _global_overwrite_decision is None:
        _global_overwrite_decision = False
        
    return _global_overwrite_decision
