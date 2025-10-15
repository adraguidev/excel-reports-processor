import os
from pathlib import Path
from typing import Tuple, Dict
import logging

# Configurar un logger simple para este módulo
logger = logging.getLogger(__name__)

class StructureValidator:
    """
    Validador y creador de la estructura de carpetas MÍNIMA necesaria
    para el funcionamiento del ejecutable Reporteador.
    """
    
    def __init__(self, base_path: str = None, log_callback=None):
        """
        Inicializa el validador de estructura.
        
        Args:
            base_path: Ruta base donde se encuentra el ejecutable/proyecto.
            log_callback: Función para registrar mensajes en la GUI.
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.log_callback = log_callback or logger.info
        
        self.structure_definition = {
            'dirs_to_create': [
                'descargas',
                'descargas/CCM',
                'descargas/PRR',
            ],
            'required_items': {
                'ASIGNACIONES': "Carpeta de configuración principal",
                'ASIGNACIONES/PERSONAL.xlsx': "Archivo de cruce de personal",
                'ASIGNACIONES/CCM.xls': "Archivo de configuración de CCM",
                'ASIGNACIONES/PRR.xls': "Archivo de configuración de PRR",
            }
        }

    def validate_and_create(self) -> Tuple[bool, Dict]:
        """
        Valida la estructura y crea las carpetas de descarga si faltan.
        
        Returns:
            Una tupla con (es_valido, reporte_detallado)
        """
        report = {
            'validation_passed': True,
            'errors': [],
            'created': []
        }
        
        self.log_callback("🔍 Validando estructura mínima del proyecto...")
        
        # 1. Crear directorios que pueden no existir
        self._create_missing_dirs(report)
        
        # 2. Validar ítems que deben existir obligatoriamente
        self._validate_required_items(report)
        
        report['validation_passed'] = not report['errors']
        
        if report['validation_passed']:
            self.log_callback("✅ Estructura mínima validada correctamente.")
        else:
            final_error_msg = "Se encontraron problemas críticos en la estructura:"
            for error in report['errors']:
                final_error_msg += f"\n- {error}"
            self.log_callback(f"❌ {final_error_msg}")

        return report['validation_passed'], report

    def _create_missing_dirs(self, report: Dict):
        """Crea los directorios de descarga si no existen."""
        self.log_callback("   - Verificando carpetas de descarga...")
        for dir_path in self.structure_definition['dirs_to_create']:
            full_path = self.base_path / dir_path
            if not full_path.exists():
                try:
                    full_path.mkdir(parents=True, exist_ok=True)
                    created_msg = f"Directorio creado: {dir_path}"
                    report['created'].append(created_msg)
                    self.log_callback(f"   - ✅ {created_msg}")
                except Exception as e:
                    error_msg = f"No se pudo crear el directorio necesario '{dir_path}': {e}"
                    report['errors'].append(error_msg)
                    self.log_callback(f"   - ❌ {error_msg}")

    def _validate_required_items(self, report: Dict):
        """Valida que los archivos y carpetas críticos existan."""
        self.log_callback("   - Verificando archivos y carpetas obligatorios...")
        for item_path, description in self.structure_definition['required_items'].items():
            full_path = self.base_path / item_path
            if not full_path.exists():
                error_msg = f"Falta el archivo/carpeta obligatorio: '{item_path}' ({description})"
                report['errors'].append(error_msg)
                self.log_callback(f"   - ❌ {error_msg}")
            else:
                self.log_callback(f"   - ✅ Encontrado: {item_path}")

def validate_project_structure(base_path: str = None, log_callback=None) -> Tuple[bool, Dict]:
    """
    Función principal simplificada para validar la estructura del proyecto.
    
    Args:
        base_path: Ruta base del proyecto.
        log_callback: Función para registrar mensajes.
        
    Returns:
        Una tupla con (es_valido, reporte_detallado)
    """
    validator = StructureValidator(base_path, log_callback)
    return validator.validate_and_create()

if __name__ == "__main__":
    # Prueba de ejecución independiente
    print("🚀 Ejecutando validación de estructura simplificada...")
    
    # Simular que 'ASIGNACIONES' no existe para forzar un error
    if os.path.exists('ASIGNACIONES'):
        print("   (Moviendo temporalmente 'ASIGNACIONES' para prueba de error)")
        os.rename('ASIGNACIONES', 'ASIGNACIONES_TEMP')
    
    success, report_with_error = validate_project_structure()
    print("\n--- PRUEBA CON ERRORES ---")
    print(f"Resultado: {'Éxito' if success else 'Fallo'}")
    if not success:
        print("Errores reportados:")
        for error in report_with_error['errors']:
            print(f"- {error}")
    
    # Restaurar 'ASIGNACIONES' si fue movida
    if os.path.exists('ASIGNACIONES_TEMP'):
        os.rename('ASIGNACIONES_TEMP', 'ASIGNACIONES')

    # Prueba con todo correcto
    print("\n--- PRUEBA CON ESTRUCTURA CORRECTA ---")
    success_ok, report_ok = validate_project_structure()
    print(f"Resultado: {'Éxito' if success_ok else 'Fallo'}")
    if report_ok['created']:
        print("Directorios creados:")
        for item in report_ok['created']:
            print(f"- {item}")
    
    if success_ok and not report_ok['created']:
        print("✅ La estructura ya estaba completa.") 