import pandas as pd

print("Python está funcionando correctamente en VS Code.")

# Prueba para abrir un Excel vacío
try:
    df = pd.DataFrame({"Estado": ["OK"], "Mensaje": ["Todo está configurado"]})
    df.to_excel("output/prueba.xlsx", index=False)
    print("Excel generado correctamente.")
except Exception as e:
    print("Error creando el Excel:", e)

from modules.securos_intermitencias import IntermitenciasProcessor

processor = IntermitenciasProcessor()
df_intermitencias = processor.procesar()

df_intermitencias.to_excel("output/intermitencias_unificadas.xlsx", index=False)

print("Archivo generado en output/intermitencias_unificadas.xlsx")

