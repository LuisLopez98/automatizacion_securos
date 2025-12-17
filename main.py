

import pandas as pd
from datetime import datetime
import shutil
from pathlib import Path

# Importar módulos
from modules.securos_intermitencias import IntermitenciasProcessor
from modules.securos_estado_grabacion import EstadoGrabacionProcessor
from modules.plantilla_excel import PlantillaExcelProcessor

print("--- INICIANDO PROCESO (ALINEADO A PLANTILLA FASE 1) ---")

# ==========================================
# FUNCIÓN: BUSCADOR FLEXIBLE DE COLUMNAS
# ==========================================
def buscar_columna_por_nombre(df, opciones):
    """Busca columna por nombre flexible (insensible a mayúsculas)."""
    cols_lower = [str(c).lower().strip() for c in df.columns]
    for opt in opciones:
        if opt in cols_lower:
            idx = cols_lower.index(opt)
            return df.columns[idx]
    return None

# ==========================================
# 1. PROCESAR INTERMITENCIAS
# ==========================================
print("\n[1/4] Procesando Intermitencias...")
proc_inter = IntermitenciasProcessor()
try:
    df_inter = proc_inter.procesar()
    
    # --- 1. Mapeo de ID FLUJO ---
    col = buscar_columna_por_nombre(df_inter, ["id flujo", "id_flujo", "flujo"])
    if col: df_inter = df_inter.rename(columns={col: "id_flujo"})
    
    # --- 2. Mapeo de ID STV (Crítico para tu plantilla) ---
    # Buscamos 'id stv' primero, si no está, usamos 'sitio'
    col = buscar_columna_por_nombre(df_inter, ["id stv", "id_stv", "sitio", "site"])
    if col: df_inter = df_inter.rename(columns={col: "id_stv"})

    # --- 3. Mapeo de ETIQUETA ---
    col = buscar_columna_por_nombre(df_inter, ["etiqueta", "nombre", "camara"])
    if col: df_inter = df_inter.rename(columns={col: "etiqueta"})

    # --- 4. Forzar ESTATUS ---
    df_inter["ESTATUS_FINAL"] = "Intermitente"
    
    # --- 5. ORDENAR PARA EXCEL ---
    # La plantilla exige: [Id flujo, Id STV, Etiqueta, Estatus]
    cols_target = ["id_flujo", "id_stv", "etiqueta", "ESTATUS_FINAL"]
    
    for c in cols_target:
        if c not in df_inter.columns: df_inter[c] = "" # Rellenar vacíos
        
    df_inter = df_inter[cols_target]
    print(f"✔ Intermitencias: {len(df_inter)} registros.")

except Exception as e:
    print(f"⚠ Error en intermitencias: {e}")
    df_inter = pd.DataFrame()

# ==========================================
# 2. PROCESAR ESTADO DE GRABACIÓN
# ==========================================
print("\n[2/4] Procesando Estado de Grabación...")
proc_estado = EstadoGrabacionProcessor()
try:
    df_estado = proc_estado.procesar()

    # --- 1. ID FLUJO ---
    col = buscar_columna_por_nombre(df_estado, ["id flujo", "id_flujo", "flujo"])
    # Lógica de respaldo: si no hay nombre, usar primera columna si no parece hora
    if not col and len(df_estado.columns) > 0:
        ejemplo = str(df_estado.iloc[0, 0]) if len(df_estado) > 0 else ""
        if ":" not in ejemplo: 
            col = df_estado.columns[0]
        elif len(df_estado.columns) > 1:
            col = df_estado.columns[1] # Probar columna B si la A es hora
            
    if col: df_estado = df_estado.rename(columns={col: "id_flujo"})

    # --- 2. ID STV ---
    col = buscar_columna_por_nombre(df_estado, ["id stv", "id_stv", "sitio", "site"])
    if col: 
        df_estado = df_estado.rename(columns={col: "id_stv"})
    else:
        df_estado["id_stv"] = "" # Crear vacía si no existe

    # --- 3. ESTATUS ---
    col = buscar_columna_por_nombre(df_estado, ["estado", "status", "estatus", "mensaje"])
    if col:
        df_estado = df_estado.rename(columns={col: "ESTATUS_FINAL"})
    else:
        df_estado["ESTATUS_FINAL"] = "Sin Información"

    # --- 4. ETIQUETA ---
    col = buscar_columna_por_nombre(df_estado, ["etiqueta", "nombre", "camara"])
    if col: df_estado = df_estado.rename(columns={col: "etiqueta"})

    # --- 5. ORDENAR PARA EXCEL ---
    cols_target = ["id_flujo", "id_stv", "etiqueta", "ESTATUS_FINAL"]
    for c in cols_target:
        if c not in df_estado.columns: df_estado[c] = ""
    
    df_estado = df_estado[cols_target]
    print(f"✔ Estados de grabación: {len(df_estado)} registros.")

except Exception as e:
    print(f"⚠ Error en estado grabación: {e}")
    df_estado = pd.DataFrame()

# ==========================================
# 3. UNIFICACIÓN
# ==========================================
print("\n[3/4] Unificando y Deduplicando...")
df_final = pd.concat([df_inter, df_estado], ignore_index=True)

# Limpieza de filas vacías
df_final = df_final[df_final["id_flujo"].notna()]
df_final = df_final[df_final["id_flujo"].astype(str).str.strip() != ""]

# Deduplicación (Mantiene el último)
df_final = df_final.drop_duplicates(subset=["id_flujo"], keep="last")

# Columna resultado (Se llena en Excel)
df_final["RESULTADO_GRABACION"] = "" 

print(f"✔ Registros listos para Excel: {len(df_final)}")

# ==========================================
# 4. EXPORTAR
# ==========================================
fecha_hoy = datetime.now().strftime("%d_%m_%Y")
nombre_plantilla = f"NUEVA_PLANTILLA_CÁM_{fecha_hoy}.xlsx"
ruta_output = Path("output") / nombre_plantilla
ruta_templates = Path("templates")

# Copiar plantilla
if not ruta_output.exists():
    bases = list(ruta_templates.glob("*.xlsx"))
    if bases:
        shutil.copy(bases[0], ruta_output)
    else:
        print("❌ ERROR: No hay plantilla .xlsx en templates/")
        exit()

print("\n[4/4] Ejecutando inserción en Excel...")
try:
    processor = PlantillaExcelProcessor(str(ruta_output), df_final)
    processor.escribir_datos_raw()
    processor.transformar_hoja_3()
    processor.actualizar_securos()
    processor.guardar()
    print("\n✅✅ PROCESO COMPLETADO EXITOSAMENTE ✅✅")

except Exception as e:
    print(f"❌ Error crítico: {e}")
