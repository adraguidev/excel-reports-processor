import pandas as pd
import numpy as np
import os
import time
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pickle
import gzip
import json

def analyze_file_structure(file_path):
    """Analiza la estructura de un archivo Excel"""
    print(f"\n📊 Analizando: {file_path}")
    
    try:
        # Leer solo las primeras filas para análisis
        df_sample = pd.read_excel(file_path, nrows=1000)
        
        # Información básica
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        print(f"   📁 Tamaño actual: {file_size:.2f} MB")
        
        # Leer archivo completo para análisis detallado
        df = pd.read_excel(file_path)
        
        print(f"   📏 Dimensiones: {df.shape[0]:,} filas x {df.shape[1]} columnas")
        print(f"   💾 Memoria en RAM: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
        
        # Análisis de tipos de datos
        print(f"   🔢 Tipos de datos:")
        for dtype in df.dtypes.value_counts().items():
            print(f"      - {dtype[0]}: {dtype[1]} columnas")
        
        # Análisis de valores nulos
        null_percentage = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
        print(f"   ❌ Valores nulos: {null_percentage:.2f}%")
        
        # Columnas con muchos valores únicos (candidatas a categorización)
        categorical_candidates = []
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:  # Menos del 50% de valores únicos
                    categorical_candidates.append((col, unique_ratio, df[col].nunique()))
        
        if categorical_candidates:
            print(f"   🏷️  Candidatas a categorización:")
            for col, ratio, unique_count in categorical_candidates[:5]:
                print(f"      - {col}: {unique_count} valores únicos ({ratio:.2%})")
        
        return df, {
            'original_size_mb': file_size,
            'shape': df.shape,
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024*1024),
            'null_percentage': null_percentage,
            'categorical_candidates': categorical_candidates
        }
        
    except Exception as e:
        print(f"   ❌ Error al analizar: {e}")
        return None, None

def optimize_dataframe(df, analysis_info):
    """Optimiza un DataFrame reduciendo el uso de memoria"""
    print(f"\n🔧 Optimizando DataFrame...")
    
    df_optimized = df.copy()
    original_memory = df.memory_usage(deep=True).sum()
    
    # 1. Convertir a categorías las columnas candidatas
    for col, ratio, unique_count in analysis_info['categorical_candidates']:
        if unique_count < 1000:  # Solo si tiene menos de 1000 valores únicos
            df_optimized[col] = df_optimized[col].astype('category')
            print(f"   ✅ {col} convertida a categoría")
    
    # 2. Optimizar tipos numéricos
    for col in df_optimized.columns:
        if df_optimized[col].dtype == 'int64':
            # Intentar reducir a int32 o int16
            col_min = df_optimized[col].min()
            col_max = df_optimized[col].max()
            
            if col_min >= np.iinfo(np.int16).min and col_max <= np.iinfo(np.int16).max:
                df_optimized[col] = df_optimized[col].astype(np.int16)
                print(f"   ✅ {col} convertida a int16")
            elif col_min >= np.iinfo(np.int32).min and col_max <= np.iinfo(np.int32).max:
                df_optimized[col] = df_optimized[col].astype(np.int32)
                print(f"   ✅ {col} convertida a int32")
        
        elif df_optimized[col].dtype == 'float64':
            # Intentar reducir a float32
            df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='float')
            if df_optimized[col].dtype == 'float32':
                print(f"   ✅ {col} convertida a float32")
    
    # 3. Optimizar fechas
    for col in df_optimized.columns:
        if 'fecha' in col.lower() or 'date' in col.lower():
            try:
                df_optimized[col] = pd.to_datetime(df_optimized[col])
                print(f"   ✅ {col} convertida a datetime")
            except:
                pass
    
    optimized_memory = df_optimized.memory_usage(deep=True).sum()
    reduction = (1 - optimized_memory / original_memory) * 100
    
    print(f"   📉 Reducción de memoria: {reduction:.1f}%")
    print(f"   💾 Memoria original: {original_memory / (1024*1024):.2f} MB")
    print(f"   💾 Memoria optimizada: {optimized_memory / (1024*1024):.2f} MB")
    
    return df_optimized

def save_in_multiple_formats(df, base_name, output_dir, produce_only_pickle_gz=False, produce_pickle=True, produce_csv_semicolon=True):
    """Guarda el DataFrame en múltiples formatos y compara tamaños"""
    
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    formats_info = {}
    
    # El parámetro `produce_only_pickle_gz` del proceso automatizado tiene prioridad
    if produce_only_pickle_gz:
        print(f"💾 Guardando únicamente en formato Pickle.GZ (proceso automático)...")
        produce_pickle = True
        produce_csv_semicolon = True # Se genera también el CSV en el proceso automático
    else:
        print(f"💾 Guardando en múltiples formatos (proceso manual)...")

    if not produce_only_pickle_gz:
        # 1. Parquet (recomendado para Streamlit)
        start_time = time.time()
        parquet_path = output_dir / f"{base_name}.parquet"
        df.to_parquet(parquet_path, compression='snappy', index=False)
        parquet_time = time.time() - start_time
        parquet_size = os.path.getsize(parquet_path) / (1024 * 1024)
        formats_info['parquet'] = {
            'size_mb': parquet_size,
            'save_time': parquet_time,
            'path': parquet_path
        }
        print(f"   ✅ Parquet: {parquet_size:.2f} MB ({parquet_time:.2f}s)")
        
        # 2. Parquet con compresión GZIP
        start_time = time.time()
        parquet_gzip_path = output_dir / f"{base_name}_gzip.parquet"
        df.to_parquet(parquet_gzip_path, compression='gzip', index=False)
        parquet_gzip_time = time.time() - start_time
        parquet_gzip_size = os.path.getsize(parquet_gzip_path) / (1024 * 1024)
        formats_info['parquet_gzip'] = {
            'size_mb': parquet_gzip_size,
            'save_time': parquet_gzip_time,
            'path': parquet_gzip_path
        }
        print(f"   ✅ Parquet GZIP: {parquet_gzip_size:.2f} MB ({parquet_gzip_time:.2f}s)")
        
        # 3. Feather (muy rápido para lectura)
        start_time = time.time()
        feather_path = output_dir / f"{base_name}.feather"
        df.to_feather(feather_path)
        feather_time = time.time() - start_time
        feather_size = os.path.getsize(feather_path) / (1024 * 1024)
        formats_info['feather'] = {
            'size_mb': feather_size,
            'save_time': feather_time,
            'path': feather_path
        }
        print(f"   ✅ Feather: {feather_size:.2f} MB ({feather_time:.2f}s)")
        
        # 4. CSV comprimido
        start_time = time.time()
        csv_gz_path = output_dir / f"{base_name}.csv.gz"
        df.to_csv(csv_gz_path, compression='gzip', index=False)
        csv_gz_time = time.time() - start_time
        csv_gz_size = os.path.getsize(csv_gz_path) / (1024 * 1024)
        formats_info['csv_gz'] = {
            'size_mb': csv_gz_size,
            'save_time': csv_gz_time,
            'path': csv_gz_path
        }
        print(f"   ✅ CSV.GZ: {csv_gz_size:.2f} MB ({csv_gz_time:.2f}s)")

    # 5. Pickle comprimido (si está seleccionado)
    if produce_pickle or produce_only_pickle_gz:
        start_time = time.time()
        pickle_gz_path = output_dir / f"{base_name}.pkl.gz"
        with gzip.open(pickle_gz_path, 'wb') as f:
            pickle.dump(df, f)
        pickle_gz_time = time.time() - start_time
        pickle_gz_size = os.path.getsize(pickle_gz_path) / (1024 * 1024)
        formats_info['pickle_gz'] = {
            'size_mb': pickle_gz_size,
            'save_time': pickle_gz_time,
            'path': pickle_gz_path
        }
        print(f"   ✅ Pickle.GZ: {pickle_gz_size:.2f} MB ({pickle_gz_time:.2f}s)")

    # 6. CSV con punto y coma (si está seleccionado)
    if produce_csv_semicolon:
        start_time = time.time()
        csv_semicolon_path = output_dir / f"{base_name}.csv"
        df.to_csv(csv_semicolon_path, sep=';', index=False, encoding='utf-8-sig')
        csv_semicolon_time = time.time() - start_time
        csv_semicolon_size = os.path.getsize(csv_semicolon_path) / (1024 * 1024)
        formats_info['csv_semicolon'] = {
            'size_mb': csv_semicolon_size,
            'save_time': csv_semicolon_time,
            'path': csv_semicolon_path
        }
        print(f"   ✅ CSV (punto y coma): {csv_semicolon_size:.2f} MB ({csv_semicolon_time:.2f}s)")
        
    return formats_info

def test_loading_speed(formats_info):
    """Prueba la velocidad de carga de cada formato"""
    print(f"\n⚡ Probando velocidad de carga...")
    
    loading_times = {}
    
    for format_name, info in formats_info.items():
        try:
            start_time = time.time()
            
            if format_name.startswith('parquet'):
                df_test = pd.read_parquet(info['path'])
            elif format_name == 'feather':
                df_test = pd.read_feather(info['path'])
            elif format_name == 'csv_gz':
                df_test = pd.read_csv(info['path'], compression='gzip')
            elif format_name == 'pickle_gz':
                with gzip.open(info['path'], 'rb') as f:
                    df_test = pickle.load(f)
            
            load_time = time.time() - start_time
            loading_times[format_name] = load_time
            print(f"   ✅ {format_name}: {load_time:.2f}s")
            
        except Exception as e:
            print(f"   ❌ {format_name}: Error - {e}")
            loading_times[format_name] = float('inf')
    
    return loading_times

def generate_streamlit_code(formats_info, loading_times):
    """Genera código de ejemplo para Streamlit"""
    
    # Encontrar el mejor formato (balance entre tamaño y velocidad)
    best_format = None
    best_score = float('inf')
    
    if not formats_info: # Si formats_info está vacío (no debería pasar si se genera al menos pickle_gz)
        return "# No se generaron formatos para crear código de Streamlit.", "error"

    for format_name, info in formats_info.items():
        if format_name in loading_times:
            # Score = tamaño normalizado + tiempo de carga normalizado
            size_score = info['size_mb'] / 100  # Normalizar por 100MB
            time_score = loading_times[format_name] / 10  # Normalizar por 10s
            total_score = size_score + time_score
            
            if total_score < best_score:
                best_score = total_score
                best_format = format_name
    
    if best_format is None: # Si no se encontró un mejor formato (ej. si loading_times estaba vacío)
        # Tomar el primer formato disponible, o pickle_gz si existe
        if 'pickle_gz' in formats_info:
            best_format = 'pickle_gz'
        else:
            best_format = list(formats_info.keys())[0] if formats_info else 'unknown'


    streamlit_code = f'''
# 🚀 Código optimizado para Streamlit

import streamlit as st
import pandas as pd
import time

@st.cache_data
def load_data(file_path):
    """Carga datos con caché de Streamlit"""
    start_time = time.time()
    
    # Formato recomendado: {best_format}
    '''
    
    if best_format.startswith('parquet'):
        streamlit_code += '''
    df = pd.read_parquet(file_path)
    '''
    elif best_format == 'feather':
        streamlit_code += '''
    df = pd.read_feather(file_path)
    '''
    elif best_format == 'csv_gz':
        streamlit_code += '''
    df = pd.read_csv(file_path, compression='gzip')
    '''
    elif best_format == 'pickle_gz':
        streamlit_code += '''
    import pickle
    import gzip
    with gzip.open(file_path, 'rb') as f:
        df = pickle.load(f)
    '''
    
    streamlit_code += f'''
    
    load_time = time.time() - start_time
    st.success(f"Datos cargados en {{load_time:.2f}} segundos")
    return df

# Uso en tu app de Streamlit
def main_streamlit(): # Renombrado para evitar conflicto con el main del script
    st.title("📊 Reporteador Optimizado")
    
    # Rutas a los archivos de datos
'''
    # Generar rutas correctas para el código de ejemplo de Streamlit
    base_name_ccm = "consolidado_final_CCM_personal"
    base_name_prr = "consolidado_final_PRR_personal"
    
    path_ccm_in_streamlit = ""
    path_prr_in_streamlit = ""

    if best_format == 'pickle_gz':
        path_ccm_in_streamlit = f"optimized/{base_name_ccm}.pkl.gz"
        path_prr_in_streamlit = f"optimized/{base_name_prr}.pkl.gz"
    elif best_format == 'csv_gz':
        path_ccm_in_streamlit = f"optimized/{base_name_ccm}.csv.gz"
        path_prr_in_streamlit = f"optimized/{base_name_prr}.csv.gz"
    elif best_format == 'feather':
        path_ccm_in_streamlit = f"optimized/{base_name_ccm}.feather"
        path_prr_in_streamlit = f"optimized/{base_name_prr}.feather"
    elif best_format == 'parquet': # Archivo es {base_name}.parquet
        path_ccm_in_streamlit = f"optimized/{base_name_ccm}.parquet"
        path_prr_in_streamlit = f"optimized/{base_name_prr}.parquet"
    elif best_format == 'parquet_gzip': # Archivo es {base_name}_gzip.parquet
        path_ccm_in_streamlit = f"optimized/{base_name_ccm}_gzip.parquet"
        path_prr_in_streamlit = f"optimized/{base_name_prr}_gzip.parquet"
    else: # Fallback por si acaso
        path_ccm_in_streamlit = f"optimized/{base_name_ccm}.{best_format.replace('_', '.')}" # Intenta construir una extensión
        path_prr_in_streamlit = f"optimized/{base_name_prr}.{best_format.replace('_', '.')}"

    streamlit_code += f'''
    # Cargar datos
    ccm_data = load_data("{path_ccm_in_streamlit}")
    prr_data = load_data("{path_prr_in_streamlit}")
    
    # Mostrar información básica
    st.subheader("Datos CCM")
    st.metric("Registros CCM", f"{{ccm_data.shape[0]:,}}")
    st.metric("Columnas CCM", ccm_data.shape[1])
    st.dataframe(ccm_data.head())

    st.subheader("Datos PRR")
    st.metric("Registros PRR", f"{{prr_data.shape[0]:,}}")
    st.metric("Columnas PRR", prr_data.shape[1])
    st.dataframe(prr_data.head())
    
    # Filtros y visualizaciones aquí...

if __name__ == "__main__":
    main_streamlit() # Llamar a la función renombrada
'''
    
    return streamlit_code, best_format

def main():
    """Función principal"""
    # CONFIGURACIÓN: Establecer en True para generar solo archivos .pkl.gz
    PRODUCE_ONLY_PICKLE_GZ = True 

    print("🚀 OPTIMIZADOR DE ARCHIVOS PARA STREAMLIT")
    print("=" * 50)
    if PRODUCE_ONLY_PICKLE_GZ:
        print("⚠️  MODO: Solo se generarán archivos Pickle.GZ (.pkl.gz)")
    
    # Rutas de los archivos
    ccm_file = "descargas/CCM/consolidado_final_CCM_personal.xlsx"
    prr_file = "descargas/PRR/consolidado_final_PRR_personal.xlsx"
    
    output_dir = "optimized"
    
    # Procesar archivo CCM
    if os.path.exists(ccm_file):
        print(f"\n🔍 PROCESANDO CCM")
        df_ccm, analysis_ccm = analyze_file_structure(ccm_file)
        
        if df_ccm is not None:
            df_ccm_optimized = optimize_dataframe(df_ccm, analysis_ccm)
            formats_ccm = save_in_multiple_formats(df_ccm_optimized, "consolidado_final_CCM_personal", output_dir, produce_only_pickle_gz=PRODUCE_ONLY_PICKLE_GZ)
            loading_times_ccm = test_loading_speed(formats_ccm)
    
    # Procesar archivo PRR
    if os.path.exists(prr_file):
        print(f"\n🔍 PROCESANDO PRR")
        df_prr, analysis_prr = analyze_file_structure(prr_file)
        
        if df_prr is not None:
            df_prr_optimized = optimize_dataframe(df_prr, analysis_prr)
            formats_prr = save_in_multiple_formats(df_prr_optimized, "consolidado_final_PRR_personal", output_dir, produce_only_pickle_gz=PRODUCE_ONLY_PICKLE_GZ)
            loading_times_prr = test_loading_speed(formats_prr)
    
    # Generar reporte final
    print(f"\n📋 REPORTE FINAL{' (Solo Pickle.GZ)' if PRODUCE_ONLY_PICKLE_GZ else ''}")
    print("=" * 50)
    
    if 'formats_ccm' in locals():
        print(f"\n📊 CCM - Comparación de formatos:")
        original_size_ccm = analysis_ccm['original_size_mb']
        
        for format_name, info in formats_ccm.items():
            reduction = (1 - info['size_mb'] / original_size_ccm) * 100
            load_time = loading_times_ccm.get(format_name, 'N/A')
            print(f"   {format_name:12}: {info['size_mb']:6.2f} MB ({reduction:+5.1f}%) - Carga: {load_time}s")
    
    if 'formats_prr' in locals():
        print(f"\n📊 PRR - Comparación de formatos:")
        original_size_prr = analysis_prr['original_size_mb']
        
        for format_name, info in formats_prr.items():
            reduction = (1 - info['size_mb'] / original_size_prr) * 100
            load_time = loading_times_prr.get(format_name, 'N/A')
            print(f"   {format_name:12}: {info['size_mb']:6.2f} MB ({reduction:+5.1f}%) - Carga: {load_time}s")
    
    # Generar código de Streamlit
    if 'formats_ccm' in locals() and 'loading_times_ccm' in locals():
        # Asegurarse de que formats_ccm no esté vacío antes de llamar a generate_streamlit_code
        if formats_ccm: 
            streamlit_code, best_format_recommendation = generate_streamlit_code(formats_ccm, loading_times_ccm)
            
            streamlit_example_file = "streamlit_optimized_example.py"
            with open(streamlit_example_file, "w", encoding="utf-8") as f:
                f.write(streamlit_code)
            
            print(f"\n🎯 RECOMENDACIÓN (basada en CCM): Usar formato '{best_format_recommendation}'")
            print(f"📝 Código de ejemplo de Streamlit guardado en: {streamlit_example_file}")
        else:
            print("\n⚠️ No se generaron formatos para CCM, no se puede crear código de ejemplo de Streamlit.")
    
    print(f"\n✅ Proceso completado. Archivos optimizados en: {Path(output_dir).resolve()}/")

if __name__ == "__main__":
    main() 