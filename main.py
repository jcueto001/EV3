from fastapi import FastAPI, Request
import uvicorn

app = FastAPI(title="Pipeline Ingesta Duoc UC - AVY1101")

@app.get("/")
def home():
    return {"status": "Servidor activo", "proyecto": "Evaluacion Parcial 3 - Big Data"}

@app.post("/webhook")
async def recibir_datos(request: Request):
    try:
        # 1. Ingesta: Capturar el JSON enviado por Duoc UC en tiempo real
        data = await request.json()
        
        # Imprime en la consola de Render para ver la estructura de la subasta
        print(f"DATOS RECIBIDOS: {data}")
        
        # Aquí es donde implementarás en el Paso 3 del informe:
        # - Validar esquema
        # - Control de duplicados (Upsert / Merge)
        # - Limpieza y normalización de texto/fechas
        
        return {"status": "success", "message": "Datos recibidos correctamente"}
    except Exception as e:
        print(f"Error procesando el POST: {str(e)}")
        return {"status": "error", "message": str(e)}, 400

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)
