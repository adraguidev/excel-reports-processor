#!/usr/bin/env python3
"""
Script para verificar la estructura mínima del proyecto Reporteador.
"""
import os
import sys
from pathlib import Path

# Añadir ruta del proyecto para poder importar el validador
sys.path.append(str(Path(__file__).parent.resolve()))

try:
    from structure_validator import validate_project_structure
except ImportError:
    print("❌ Error: No se pudo importar 'structure_validator.py'.")
    print("   Asegúrate de que el script esté en la carpeta principal del proyecto.")
    sys.exit(1)

def run_check():
    """Ejecuta la validación y muestra un reporte claro."""
    print("=" * 60)
    print("🔍 VERIFICADOR DE ESTRUCTURA PARA REPORTEADOR")
    print("=" * 60)
    print(f"Directorio actual: {os.getcwd()}\n")

    is_valid, report = validate_project_structure()

    if report['created']:
        print("🛠️  Elementos creados automáticamente:")
        for item in report['created']:
            print(f"  [+] {item}")
        print("-" * 30)

    if is_valid:
        print("✅ ¡ÉXITO! La estructura del proyecto es correcta.")
        print("   La aplicación debería funcionar sin problemas.")
    else:
        print("❌ ¡ERROR CRÍTICO! Faltan archivos o carpetas indispensables.")
        print("   La aplicación NO podrá iniciarse correctamente.")
        print("\n   Por favor, asegúrate de que los siguientes elementos existan:")
        for error in report['errors']:
            item_name = error.split("'")[1] if "'" in error else error
            print(f"   - {item_name}")
        
        print("\n   La aplicación se cerrará si no se resuelven estos problemas.")

    print("\n" + "=" * 60)

if __name__ == "__main__":
    run_check() 