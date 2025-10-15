# main.py - Aplicación de descarga de archivos

import os
import sys
import json
from dotenv import load_dotenv
from gui.main_window import MainWindow, messagebox
from utils_logging import logger
from structure_validator import validate_project_structure

# Cargar variables de entorno (si existen)
load_dotenv()

def run_pre_startup_validation():
    """
    Ejecuta una validación crítica antes de iniciar la GUI.
    Si falla, muestra un error y cierra la aplicación.
    """
    print("🔍 Validando estructura de directorios y archivos críticos...")
    
    is_valid, report = validate_project_structure()
    
    if is_valid:
        print("✅ Estructura base validada correctamente.")
        # Configurar variables de entorno solo después de una validación exitosa
        os.environ.setdefault('DOWNLOAD_MAX_WORKERS', '7')
        os.environ.setdefault('DOWNLOAD_DELAY', '1')
        os.environ.setdefault('CHUNK_SIZE', '8192')
        os.environ.setdefault('DIRECT_DOWNLOAD', 'true')
        
        # Cargar configuración de módulos desde variables de entorno
        default_modules = {'CCM': True, 'PRR': True}
        selected_modules_str = os.getenv('SELECTED_MODULES')
        if selected_modules_str:
            try:
                selected_modules = json.loads(selected_modules_str)
                # Asegurar que solo se incluyan los módulos válidos
                selected_modules = {k: v for k, v in selected_modules.items() if k in default_modules}
            except json.JSONDecodeError:
                selected_modules = default_modules
        else:
            selected_modules = default_modules
        os.environ['SELECTED_MODULES'] = json.dumps(selected_modules)
        
        return True
    else:
        # Si la validación falla, construir un mensaje de error detallado
        error_message = "Error Crítico: Faltan archivos o carpetas indispensables.\n\n"
        error_message += "Por favor, asegúrate de que existan los siguientes elementos en la carpeta del programa:\n"
        for error in report['errors']:
            # Simplificar el mensaje para el usuario final
            item_name = error.split("'")[1] if "'" in error else error
            error_message += f"\n- {item_name}"
        
        error_message += "\n\nLa aplicación no puede continuar y se cerrará."
        
        # Usar tkinter para mostrar el mensaje de error antes de que la app principal se cree
        root = MainWindow(run_validation=False) # Crear una ventana raíz temporal
        root.withdraw() # Ocultarla
        messagebox.showerror("Error de Configuración", error_message)
        root.destroy()
        
        print(f"❌ {error_message}")
        return False

def main():
    """
    Función principal que inicia la interfaz gráfica para la descarga de archivos.
    """
    # Verificar si existen credenciales guardadas localmente
    from credentials_manager import credentials_manager
    
    if credentials_manager.has_credentials():
        user, _ = credentials_manager.get_credentials()
        logger.info(f"Credenciales NTLM cargadas desde archivo local para usuario: {user}")
    else:
        logger.info("No se encontraron credenciales NTLM guardadas. Se pueden configurar en la interfaz.")

    if run_pre_startup_validation():
        logger.info("Iniciando aplicación de descarga")
        app = MainWindow() 
        app.mainloop()
        logger.info("Finalizando aplicación de descarga")

if __name__ == "__main__":
    main()
