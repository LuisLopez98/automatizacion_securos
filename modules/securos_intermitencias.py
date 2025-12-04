import pandas as pd
import os

class IntermitenciasProcessor:

    def __init__(self, carpeta="data_raw/intermitencias"):
        self.carpeta = carpeta

    # ----------------------------------------------------------
    # Detecta la fila donde realmente están los encabezados
    # ----------------------------------------------------------
    def detectar_encabezado(self, path):
        posibles_cols = ["sitio", "id stv", "id flujo", "etiqueta", "eventos"]

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for idx, line in enumerate(f):
                linea = line.lower().strip()
                match = sum(1 for c in posibles_cols if c in linea)
                if match >= 2:
                    return idx  # fila donde sí hay encabezado válido

        raise Exception(f"No se encontró encabezado válido en {os.path.basename(path)}")

    # ----------------------------------------------------------
    # Procesa un archivo CSV individual
    # ----------------------------------------------------------
    def procesar_archivo(self, path):
        header_row = self.detectar_encabezado(path)

        df = pd.read_csv(
            path,
            skiprows=header_row,
            header=0,
            encoding="utf-8",
            on_bad_lines="skip"
        )

        # Normalizar columnas
        df.columns = df.columns.str.strip().str.lower()

        # Renombrar a estándar
        mapping = {
            "sitio": "sitio",
            "id stv": "id_stv",
            "id flujo": "id_flujo",
            "etiqueta": "etiqueta",
            "eventos": "eventos"
        }

        df = df.rename(columns=mapping)

        # Mantener solo lo relevante
        columnas_finales = ["sitio", "id_stv", "id_flujo", "etiqueta", "eventos"]
        df = df[[c for c in columnas_finales if c in df.columns]]

        return df

    # ----------------------------------------------------------
    # Procesa todos los archivos dentro de /input
    # ----------------------------------------------------------
    def procesar(self):
        archivos = [a for a in os.listdir(self.carpeta) if a.lower().endswith(".csv")]

        if not archivos:
            raise Exception(f"No hay archivos CSV en la carpeta {self.carpeta}")

        dfs = []

        print("📂 Cargando archivos...")

        for archivo in archivos:
            path = os.path.join(self.carpeta, archivo)
            print(f"📄 Procesando archivo: {archivo}")
            df = self.procesar_archivo(path)
            dfs.append(df)

        print("🔗 Unificando archivos...")

        df_total = pd.concat(dfs, ignore_index=True)

        print("✔ Procesado completo")

        return df_total
