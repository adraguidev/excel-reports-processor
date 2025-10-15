import tkinter as tk
from tkinter import ttk, messagebox
import ttkbootstrap as tb
from ttkbootstrap.constants import *
import threading
import queue
import os
from pathlib import Path
import pandas as pd
import numpy as np
import pickle
import gzip
import time

class OptimizeFilesDialog(tb.Toplevel):
    """Ventana de diálogo para optimizar archivos a formato Pickle.GZ"""
    
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        
        self.title("Optimizar Archivos para Streamlit")
        self.geometry("700x650")
        self.resizable(True, True)
        self.transient(parent)
        self.grab_set()
        
        # Centrar la ventana
        self.geometry("+%d+%d" % (parent.winfo_rootx() + 50, parent.winfo_rooty() + 50))
        
        self.queue = queue.Queue()
        self.is_running = False
        self.optimization_thread = None
        
        self.create_widgets()
        
    def create_widgets(self):
        # Frame principal
        main_frame = tb.Frame(self, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = tb.Label(main_frame, text="🚀 Optimizador de Archivos para Streamlit", 
                               font=('Helvetica', 14, 'bold'))
        title_label.pack(pady=(0, 20))
        
        # Información
        info_frame = tb.LabelFrame(main_frame, text="Información", padding="10")
        info_frame.pack(fill=tk.X, pady=(0, 15))
        
        info_text = """Esta herramienta optimiza tus archivos consolidados Excel a formato Pickle.GZ:
        
✅ Reduce el tamaño hasta 91% (de ~100MB a ~9MB)
✅ Carga 20-60x más rápida en Streamlit
✅ Optimiza tipos de datos y memoria RAM
✅ Compatible con @st.cache_data
        
Los archivos optimizados se guardarán en la carpeta 'optimized/'"""
        
        tb.Label(info_frame, text=info_text, justify=tk.LEFT).pack(anchor=tk.W)
        
        # Opciones de Formato
        format_frame = tb.LabelFrame(main_frame, text="Formatos de Salida", padding="10")
        format_frame.pack(fill=tk.X, pady=(0, 15))
        
        self.pickle_var = tk.BooleanVar(value=True)
        self.csv_var = tk.BooleanVar(value=True)
        
        tb.Checkbutton(format_frame, text="Pickle.GZ (Recomendado para Streamlit)", variable=self.pickle_var, bootstyle="primary").pack(anchor=tk.W)
        tb.Checkbutton(format_frame, text="CSV (Delimitado por ;)", variable=self.csv_var, bootstyle="primary").pack(anchor=tk.W)
        
        # Estado de archivos
        status_frame = tb.LabelFrame(main_frame, text="Estado de Archivos", padding="10")
        status_frame.pack(fill=tk.X, pady=(0, 15))
        
        self.ccm_status = tb.Label(status_frame, text="CCM: Verificando...", bootstyle="info")
        self.ccm_status.pack(anchor=tk.W)
        
        self.prr_status = tb.Label(status_frame, text="PRR: Verificando...", bootstyle="info")
        self.prr_status.pack(anchor=tk.W)
        
        # Log de progreso
        log_frame = tb.LabelFrame(main_frame, text="Progreso", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))
        
        self.log_text = tk.Text(log_frame, height=10, wrap=tk.WORD, state=tk.DISABLED)
        scrollbar = tb.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview, bootstyle="round")
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Barra de progreso
        self.progress_var = tk.DoubleVar()
        self.progress_bar = tb.Progressbar(main_frame, variable=self.progress_var, 
                                           maximum=100, length=500, bootstyle="success-striped")
        self.progress_bar.pack(fill=tk.X, pady=(5, 15))
        
        # Botones - Asegurar que siempre estén visibles
        button_frame = tb.Frame(main_frame)
        button_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=(10, 0))
        
        self.optimize_button = tb.Button(button_frame, text="🚀 Optimizar Archivos", 
                                         command=self.start_optimization, bootstyle="success")
        self.optimize_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.cancel_button = tb.Button(button_frame, text="Cancelar", 
                                       command=self.cancel_optimization, state=tk.DISABLED, bootstyle="danger")
        self.cancel_button.pack(side=tk.LEFT, padx=(0, 10))
        
        tb.Button(button_frame, text="Cerrar", command=self.close_dialog, bootstyle="secondary").pack(side=tk.RIGHT)
        
        # Verificar archivos al inicio
        self.check_files()
        self.check_queue()
        
    def check_files(self):
        """Verifica si existen los archivos a optimizar"""
        ccm_file = "descargas/CCM/consolidado_final_CCM_personal.xlsx"
        prr_file = "descargas/PRR/consolidado_final_PRR_personal.xlsx"
        
        if os.path.exists(ccm_file):
            size_mb = os.path.getsize(ccm_file) / (1024 * 1024)
            self.ccm_status.configure(text=f"✅ CCM: Encontrado ({size_mb:.1f} MB)", bootstyle="success")
        else:
            self.ccm_status.configure(text="❌ CCM: No encontrado", bootstyle="danger")
            
        if os.path.exists(prr_file):
            size_mb = os.path.getsize(prr_file) / (1024 * 1024)
            self.prr_status.configure(text=f"✅ PRR: Encontrado ({size_mb:.1f} MB)", bootstyle="success")
        else:
            self.prr_status.configure(text="❌ PRR: No encontrado", bootstyle="danger")
            
    def log_message(self, message):
        """Añade un mensaje al log"""
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.configure(state=tk.DISABLED)
        self.log_text.see(tk.END)
        
    def start_optimization(self):
        """Inicia el proceso de optimización"""
        if self.is_running:
            return
            
        self.is_running = True
        self.optimize_button.configure(state=tk.DISABLED)
        self.cancel_button.configure(state=tk.NORMAL)
        self.progress_var.set(0)
        
        # Limpiar log
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state=tk.DISABLED)
        
        self.log_message("🚀 Iniciando optimización de archivos...")
        
        def optimization_worker():
            try:
                # Importar optimize_files para usar sus funciones
                import sys
                sys.path.append('.')
                from optimize_files import analyze_file_structure, optimize_dataframe, save_in_multiple_formats
                
                # Obtener formatos seleccionados
                produce_pickle = self.pickle_var.get()
                produce_csv = self.csv_var.get()

                if not produce_pickle and not produce_csv:
                    self.queue.put(("error", "Debes seleccionar al menos un formato de salida."))
                    return

                files_to_process = [
                    ("descargas/CCM/consolidado_final_CCM_personal.xlsx", "CCM"),
                    ("descargas/PRR/consolidado_final_PRR_personal.xlsx", "PRR")
                ]
                
                total_files = len([f for f, _ in files_to_process if os.path.exists(f)])
                if total_files == 0:
                    self.queue.put(("error", "No se encontraron archivos para optimizar"))
                    return
                    
                processed_files = 0
                output_dir = "optimized"
                
                for file_path, file_type in files_to_process:
                    if not os.path.exists(file_path):
                        continue
                        
                    try:
                        self.queue.put(("log", f"📊 Analizando archivo {file_type}..."))
                        
                        # Analizar estructura
                        df, analysis_info = analyze_file_structure(file_path)
                        if df is None:
                            self.queue.put(("error", f"Error al leer archivo {file_type}"))
                            continue
                            
                        self.queue.put(("log", f"   📏 {df.shape[0]:,} filas × {df.shape[1]} columnas"))
                        self.queue.put(("log", f"   💾 Tamaño en memoria: {analysis_info['memory_usage_mb']:.1f} MB"))
                        
                        # Optimizar DataFrame
                        self.queue.put(("log", f"🔧 Optimizando {file_type}..."))
                        df_optimized = optimize_dataframe(df, analysis_info)
                        
                        # Guardar solo en formato Pickle.GZ
                        self.queue.put(("log", f"💾 Guardando {file_type} en formatos seleccionados..."))
                        
                        # Guardar en los formatos seleccionados usando la función centralizada
                        formats_info = save_in_multiple_formats(
                            df_optimized, 
                            f"consolidado_final_{file_type}_personal", 
                            output_dir, 
                            produce_pickle=produce_pickle,
                            produce_csv_semicolon=produce_csv
                        )
                        
                        # Log de resultados
                        if 'pickle_gz' in formats_info:
                            original_size = analysis_info['original_size_mb']
                            optimized_size = formats_info['pickle_gz']['size_mb']
                            reduction = (1 - optimized_size / original_size) * 100
                            self.queue.put(("log", f"   ✅ Pickle.GZ: {optimized_size:.2f} MB (-{reduction:.1f}%)"))

                        if 'csv_semicolon' in formats_info:
                            csv_size = formats_info['csv_semicolon']['size_mb']
                            self.queue.put(("log", f"   ✅ CSV (;): {csv_size:.2f} MB"))

                        processed_files += 1
                        progress = (processed_files / total_files) * 100
                        self.queue.put(("progress", progress))
                        
                    except Exception as e:
                        self.queue.put(("error", f"Error procesando {file_type}: {str(e)}"))
                        continue
                
                if processed_files > 0:
                    self.queue.put(("log", f"\n🎉 Optimización completada!"))
                    self.queue.put(("log", f"📁 Archivos guardados en: {Path(output_dir).resolve()}"))
                    self.queue.put(("log", f"🚀 Usa estos archivos en Streamlit para máximo rendimiento"))
                else:
                    self.queue.put(("error", "No se pudo procesar ningún archivo"))
                    
            except Exception as e:
                self.queue.put(("error", f"Error general: {str(e)}"))
            finally:
                self.queue.put(("finished", None))
                
        self.optimization_thread = threading.Thread(target=optimization_worker)
        self.optimization_thread.start()
        
    def cancel_optimization(self):
        """Cancela la optimización"""
        if self.is_running:
            self.is_running = False
            self.log_message("❌ Optimización cancelada por el usuario")
            self.optimization_finished()
            
    def optimization_finished(self):
        """Finaliza el proceso de optimización"""
        self.is_running = False
        self.optimize_button.configure(state=tk.NORMAL)
        self.cancel_button.configure(state=tk.DISABLED)
        
    def check_queue(self):
        """Verifica mensajes en la cola"""
        try:
            while True:
                msg_type, msg_data = self.queue.get_nowait()
                if msg_type == "log":
                    self.log_message(msg_data)
                elif msg_type == "progress":
                    self.progress_var.set(msg_data)
                elif msg_type == "error":
                    self.log_message(f"❌ ERROR: {msg_data}")
                    messagebox.showerror("Error", msg_data)
                    self.optimization_finished()
                elif msg_type == "finished":
                    self.optimization_finished()
                    messagebox.showinfo("Completado", "Optimización finalizada exitosamente!")
        except queue.Empty:
            if self.is_running:
                self.after(100, self.check_queue)
            else:
                self.after(1000, self.check_queue)  # Verificar menos frecuentemente cuando no está corriendo
                
    def close_dialog(self):
        """Cierra el diálogo"""
        if self.is_running:
            if messagebox.askyesno("Confirmar", "¿Cancelar la optimización y cerrar?"):
                self.cancel_optimization()
                self.destroy()
        else:
            self.destroy()

def add_optimize_button_to_processing_tab(processing_tab, queue_instance, log_callback):
    """Añade el botón de optimización a la pestaña de procesamiento"""
    
    def open_optimize_dialog():
        """Abre el diálogo de optimización"""
        # Verificar que la ventana principal esté disponible
        root = processing_tab.winfo_toplevel()
        dialog = OptimizeFilesDialog(root)
        
    # Buscar el frame de botones existente
    for widget in processing_tab.winfo_children():
        if isinstance(widget, ttk.Frame):
            # Verificar si es el frame de botones (tiene botones como hijos)
            has_buttons = any(isinstance(child, ttk.Button) for child in widget.winfo_children())
            if has_buttons:
                # Añadir el botón de optimización
                optimize_button = ttk.Button(widget, text="🚀 Optimizar para Streamlit", 
                                           command=open_optimize_dialog, style='TButton')
                # Encontrar la próxima columna disponible
                button_count = len([child for child in widget.winfo_children() if isinstance(child, ttk.Button)])
                optimize_button.grid(row=0, column=button_count, padx=10)
                break

if __name__ == "__main__":
    # Ejemplo de uso independiente
    root = tk.Tk()
    root.title("Test Optimizador")
    root.geometry("400x300")
    
    def test_open_dialog():
        dialog = OptimizeFilesDialog(root)
    
    ttk.Button(root, text="Abrir Optimizador", command=test_open_dialog).pack(pady=50)
    
    root.mainloop() 