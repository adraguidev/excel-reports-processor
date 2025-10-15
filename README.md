# 📊 Reporteador - Sistema de Descarga y Procesamiento de Reportes

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Status](https://img.shields.io/badge/Status-Activo-brightgreen.svg)

Una aplicación de escritorio desarrollada en Python para la descarga automática, consolidación y procesamiento de reportes desde servidores corporativos. El sistema incluye una interfaz gráfica moderna y funcionalidades avanzadas de optimización de archivos.

## 🚀 Características Principales

### 📥 Descarga Automática
- **Descarga paralela** con múltiples workers para mayor eficiencia
- **Autenticación NTLM** para acceso a servidores corporativos
- **Manejo inteligente de archivos** con detección de duplicados
- **Throttling configurable** para evitar sobrecarga del servidor
- **Soporte para múltiples módulos**: CCM, PRR, SOL

### 🔧 Procesamiento de Datos
- **Consolidación automática** de archivos CSV/Excel
- **Optimización de archivos** con múltiples formatos (Excel, Parquet, Pickle)
- **Procesamiento en paralelo** para mejorar rendimiento
- **Análisis de estructura** de archivos para optimización

### 🎨 Interfaz Gráfica
- **Diseño moderno** con ttkbootstrap y tema superhero
- **Progreso en tiempo real** con barras de progreso y logs
- **Gestión de credenciales** segura y persistente
- **Validación de estructura** automática al inicio

## 📋 Requisitos del Sistema

- **Python**: 3.8 o superior
- **Sistema Operativo**: Windows (optimizado para entornos corporativos)
- **Memoria RAM**: Mínimo 4GB (recomendado 8GB+)
- **Espacio en disco**: Variable según el volumen de datos a procesar

## 🛠️ Instalación

### 1. Clonar el Repositorio
```bash
git clone https://github.com/tu-usuario/reporteador_project.git
cd reporteador_project
```

### 2. Crear Entorno Virtual
```bash
python -m venv venv
venv\Scripts\activate  # En Windows
```

### 3. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 4. Configurar Variables de Entorno (Opcional)
Crear un archivo `.env` en la raíz del proyecto:
```env
DOWNLOAD_MAX_WORKERS=7
DOWNLOAD_DELAY=1
CHUNK_SIZE=8192
DIRECT_DOWNLOAD=true
REPORT_BASE_URL=http://172.27.230.27/ReportServer
REQUEST_TIMEOUT=600
```

## 🚀 Uso

### Ejecutar la Aplicación
```bash
python main.py
```

### Funcionalidades Principales

#### 1. Configuración de Credenciales
- Al iniciar por primera vez, configurar usuario y contraseña NTLM
- Las credenciales se guardan de forma segura para futuras sesiones

#### 2. Descarga de Reportes
- Seleccionar módulos a descargar (CCM, PRR, SOL)
- Configurar rango de fechas
- Iniciar descarga con control de progreso en tiempo real

#### 3. Procesamiento de Archivos
- Consolidación automática de archivos descargados
- Optimización de archivos Excel para mejor rendimiento
- Exportación a múltiples formatos

#### 4. Gestión de Archivos
- Análisis de estructura de archivos
- Compresión y optimización automática
- Backup automático de datos procesados

## 📁 Estructura del Proyecto

```
reporteador_project/
├── 📁 gui/                    # Interfaz gráfica
│   ├── main_window.py         # Ventana principal
│   └── download_manager.py    # Gestión de descargas
├── 📁 assets/                 # Recursos gráficos
│   ├── reporteador.ico        # Icono de la aplicación
│   └── reporteador.png        # Imágenes
├── 📄 main.py                 # Punto de entrada principal
├── 📄 descarga.py             # Lógica de descarga
├── 📄 excel_processor.py      # Procesamiento de Excel
├── 📄 optimize_files.py       # Optimización de archivos
├── 📄 credentials_manager.py  # Gestión de credenciales
└── 📄 requirements.txt        # Dependencias del proyecto
```

## 🔧 Configuración Avanzada

### Variables de Entorno

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `DOWNLOAD_MAX_WORKERS` | Número de workers paralelos | 7 |
| `DOWNLOAD_DELAY` | Delay entre descargas (segundos) | 1 |
| `CHUNK_SIZE` | Tamaño de chunk para descarga | 8192 |
| `DIRECT_DOWNLOAD` | Descarga directa sin verificación | true |
| `REPORT_BASE_URL` | URL base del servidor de reportes | http://172.27.230.27/ReportServer |
| `REQUEST_TIMEOUT` | Timeout para requests (segundos) | 600 |

### Personalización de Módulos
```python
# En main.py, línea 32
default_modules = {'CCM': True, 'PRR': True, 'SOL': False}
```

## 📊 Formatos de Salida

### Excel Optimizado
- **Formato**: .xlsx con tablas dinámicas
- **Compresión**: Automática para archivos grandes
- **Formato**: Estilos y formato profesional

### Parquet
- **Ventaja**: Compresión superior y lectura rápida
- **Uso**: Ideal para análisis de datos grandes
- **Compatibilidad**: Pandas, Apache Arrow

### Pickle Comprimido
- **Ventaja**: Preserva tipos de datos exactos
- **Compresión**: Gzip para reducir tamaño
- **Uso**: Para procesamiento interno rápido

## 🔒 Seguridad

- **Credenciales**: Almacenadas localmente con cifrado
- **Autenticación NTLM**: Para acceso a servidores corporativos
- **Validación**: Verificación de estructura de archivos
- **Logs**: Registro detallado de operaciones

## 🐛 Solución de Problemas

### Error de Credenciales
```
No se encontraron credenciales NTLM válidas
```
**Solución**: Configurar credenciales en la interfaz gráfica

### Error de Estructura
```
Faltan archivos o carpetas indispensables
```
**Solución**: Verificar que todas las carpetas estén presentes

### Problemas de Memoria
```
MemoryError durante el procesamiento
```
**Solución**: Reducir `DOWNLOAD_MAX_WORKERS` o procesar archivos en lotes más pequeños

## 📈 Rendimiento

### Optimizaciones Implementadas
- **Descarga paralela** con ThreadPoolExecutor
- **Procesamiento en chunks** para archivos grandes
- **Gestión de memoria** con garbage collection
- **Compresión automática** de archivos de salida

### Métricas Típicas
- **Velocidad de descarga**: 50-100 archivos/minuto
- **Compresión**: 60-80% de reducción de tamaño
- **Tiempo de procesamiento**: Variable según volumen de datos

## 🤝 Contribución

1. Fork el proyecto
2. Crear una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir un Pull Request

## 📝 Changelog

### v2.0.0
- ✅ Interfaz gráfica moderna con ttkbootstrap
- ✅ Descarga paralela optimizada
- ✅ Gestión de credenciales mejorada
- ✅ Validación de estructura automática
- ✅ Procesamiento de archivos optimizado

### v1.0.0
- ✅ Funcionalidad básica de descarga
- ✅ Consolidación de archivos
- ✅ Interfaz de línea de comandos

## 📄 Licencia

Este proyecto está bajo la Licencia MIT.

## 👥 Autores

- **Adrian Aguirre Barrionuevo**

---

**⭐ Si este proyecto te ha sido útil, considera darle una estrella en GitHub!**