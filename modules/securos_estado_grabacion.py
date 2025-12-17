import os
import pandas as pd

class EstadoGrabacionProcessor:
    def __init__(self, carpeta="data_raw/estado_grabacion"):
        self.carpeta = carpeta

    # ----------------------------------------------------------
    # 1. DETECTOR INTELIGENTE DE ENCABEZADOS (NUEVO)
    # ----------------------------------------------------------
    def detectar_fila_encabezado(self, ruta_archivo):
        """
        Lee las primeras 20 líneas del archivo para encontrar dónde empiezan los datos reales.
        Busca palabras clave como 'sitio', 'id flujo', 'estado', etc.
        """
        palabras_clave = ["sitio", "id flujo", "id_flujo", "etiqueta", "estado", "status", "grabacion"]
        
        try:
            with open(ruta_archivo, "r", encoding="utf-8", errors="ignore") as f:
                for i, linea in enumerate(f):
                    linea_lower = linea.lower()
                    # Si la línea tiene al menos 2 palabras clave, es el encabezado
                    coincidencias = sum(1 for p in palabras_clave if p in linea_lower)
                    if coincidencias >= 2:
                        return i
                    if i > 20: # Si en 20 líneas no encuentra nada, asumimos fila 0
                        break
        except Exception:
            pass
        return 0 # Por defecto fila 0

    # ----------------------------------------------------------
    # 2. CARGAR ARCHIVOS
    # ----------------------------------------------------------
    def cargar_archivos(self):
        archivos = [f for f in os.listdir(self.carpeta) if f.lower().endswith(".csv")]

        if not archivos:
            raise FileNotFoundError("No se encontraron archivos CSV en la carpeta de estado de grabación.")

        dataframes = []
        print(f"   📂 Encontrados {len(archivos)} archivos de Estado de Grabación.")

        for archivo in archivos:
            ruta = os.path.join(self.carpeta, archivo)
            
            # Usamos el detector para saltar la basura del inicio
            fila_header = self.detectar_fila_encabezado(ruta)
            
            try:
                # Leemos saltando las filas basura (skiprows)
                df = pd.read_csv(ruta, skiprows=fila_header, encoding="utf-8", on_bad_lines="skip")
                
                # Limpieza básica de columnas (quitar espacios en blanco en los nombres)
                df.columns = df.columns.str.strip()
                
                # Filtro de seguridad: Si el DataFrame está vacío o casi vacío, lo saltamos
                if not df.empty and len(df.columns) > 1:
                    dataframes.append(df)
                    
            except Exception as e:
                print(f"   ⚠ Error leyendo {archivo}: {e}")

        return dataframes

    # ----------------------------------------------------------
    # 3. UNIFICAR
    # ----------------------------------------------------------
    def unificar(self):
        dataframes = self.cargar_archivos()
        if not dataframes:
            return pd.DataFrame() # Retorna vacío si falló todo
            
        df_unificado = pd.concat(dataframes, ignore_index=True)
        return df_unificado

    # ----------------------------------------------------------
    # 4. PROCESAR (Punto de entrada)
    # ----------------------------------------------------------
    def procesar(self):
        df = self.unificar()
        
        # Normalización preliminar de nombres de columnas para facilitar el trabajo a main.py
        # Convertimos todo a minúsculas para buscar mejor
        df.columns = df.columns.str.lower()
        
        return df
