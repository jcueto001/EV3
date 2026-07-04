from flask import Flask, request, jsonify, Response
from datetime import datetime
import json
import os
import csv
from io import StringIO

app = Flask(__name__)

# Archivo de persistencia local (Capa Cruda / Data Lake)
ARCHIVO_DATOS = os.path.join(os.getcwd(), "subastas_tiempo_real.jsonl")

# Columnas finales estandarizadas para tu informe y reporte CSV
COLUMNAS_CSV = [
    "fecha_recepcion_api",
    "fecha_registro",
    "id_item",
    "nombre_item",
    "categoria_enriquecida",
    "precio_valor",
    "cantidad_volumen",
    "monto_total_calculado",
    "zona_geografica",
    "estado_validacion",
    "observaciones_calidad"
]

@app.route("/", methods=["GET"])
def inicio():
    return "Pipeline Flexible de Big Data Duoc UC (AVY1101) - Activo", 200

@app.route("/webhook", methods=["POST", "GET", "HEAD"])
def recibir_datos():
    """
    IL 3.1: Ingesta en línea ultra flexible. Acepta cualquier método de verificación
    y guarda TODO JSON entrante sin filtros restrictivos para asegurar la captura.
    """
    if request.method in ["GET", "HEAD"]:
        return jsonify({
            "estado": "activo",
            "mensaje": "Endpoint flexible listo y escuchando transmisiones de Duoc UC."
        }), 200

    cuerpo_crudo = request.get_data(as_text=True)

    if not cuerpo_crudo or not cuerpo_crudo.strip():
        return jsonify({"estado": "rechazado", "error": "Cuerpo vacío"}), 400

    try:
        estructura_json = json.loads(cuerpo_crudo)
    except json.JSONDecodeError:
        return jsonify({"estado": "rechazado", "error": "JSON inválido"}), 400

    # Normalizar la entrada para que siempre sea una lista de objetos, sin importar el formato
    eventos = []
    if isinstance(estructura_json, list):
        eventos = estructura_json
    elif isinstance(estructura_json, dict):
        if "data" in estructura_json:
            contenido = estructura_json["data"]
            eventos = contenido if isinstance(contenido, list) else [contenido]
        else:
            eventos = [estructura_json]

    if not eventos:
        return jsonify({"estado": "rechazado", "error": "No se encontraron registros en el JSON"}), 400

    fecha_recepcion_sistema = datetime.now().astimezone().isoformat()

    # Guardar el lote crudo tal cual llegó (Garantiza que no se pierda nada por esquema)
    registro_auditoria = {
        "fecha_recepcion": fecha_recepcion_sistema,
        "data": eventos
    }

    print(f"Streaming Recibido: {len(eventos)} registro(s) capturado(s) - {fecha_recepcion_sistema}", flush=True)

    try:
        with open(ARCHIVO_DATOS, "a", encoding="utf-8") as archivo:
            archivo.write(json.dumps(registro_auditoria, ensure_ascii=False) + "\n")
    except OSError as error_sistema:
        print(f"Error crítico de escritura: {error_sistema}", flush=True)
        return jsonify({"estado": "error", "error": "Fallo en la persistencia local"}), 500

    return jsonify({
        "estado": "recibido",
        "mensaje": "Datos capturados con éxito por el pipeline flexible",
        "fecha_recepcion": fecha_recepcion_sistema,
        "registros_lote_actual": len(eventos),
        "total_lotes_acumulados": contar_registros_crudos()
    }), 200

@app.route("/datos-limpios", methods=["GET"])
def datos_limpios():
    filas_procesadas = obtener_datos_procesados()
    return jsonify({
        "capa_datos": "Silver/Gold - Consumo Flexible",
        "total_registros_limpios": len(filas_procesadas),
        "datos": filas_procesadas
    }), 200

@app.route("/descargar-csv", methods=["GET"])
def descargar_csv():
    filas_procesadas = obtener_datos_procesados()
    string_memoria = StringIO()
    escritor_csv = csv.DictWriter(string_memoria, fieldnames=COLUMNAS_CSV)
    
    escritor_csv.writeheader()
    escritor_csv.writerows(filas_procesadas)
    
    return Response(
        string_memoria.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=subastas_limpias_bigdata.csv"}
    )

@app.route("/resumen", methods=["GET"])
def resumen():
    """
    IL 3.3: Mapeo dinámico de métricas para el Dashboard.
    """
    filas_procesadas = obtener_datos_procesados()

    monto_total = sum(f["monto_total_calculado"] for f in filas_procesadas if isinstance(f["monto_total_calculado"], (int, float)))
    unidades_totales = sum(f["cantidad_volumen"] for f in filas_procesadas if isinstance(f["cantidad_volumen"], int))

    metricas_items = {}
    metricas_zona = {}

    for fila in filas_procesadas:
        item_nom = fila["nombre_item"] or "Desconocido"
        zona = fila["zona_geografica"] or "Zona General"
        
        metricas_items[item_nom] = metricas_items.get(item_nom, 0) + 1
        metricas_zona[zona] = metricas_zona.get(zona, 0) + 1

    return jsonify({
        "indicador_logro": "IL 3.3 - Resumen Ejecutivo",
        "total_registros_analizados": len(filas_procesadas),
        "volumen_total_unidades": unidades_totales,
        "valor_total_calculado": round(monto_total, 2),
        "frecuencia_por_item": metricas_items,
        "frecuencia_por_zona": metricas_zona
    }), 200


# =====================================================================
# MOTOR DE TRADUCCIÓN Y MAPEO DINÁMICO (IL 3.2)
# =====================================================================

def leer_registros_crudos():
    if not os.path.exists(ARCHIVO_DATOS):
        return []
    datos_acumulados = []
    try:
        with open(ARCHIVO_DATOS, "r", encoding="utf-8") as archivo:
            for linea in archivo:
                if linea.strip():
                    datos_acumulados.append(json.loads(linea))
    except OSError:
        pass
    return datos_acumulados

def contar_registros_crudos():
    return len(leer_registros_crudos())

def obtener_datos_procesados():
    """
    IL 3.2: Limpieza y normalización sin importar las variantes en los nombres de las claves.
    """
    registros_crudos = leer_registros_crudos()
    filas_finales = []
    registro_duplicados = set()

    for registro in registros_crudos:
        fecha_recepcion_api = registro.get("fecha_recepcion", "")
        items_lote = registro.get("data", [])

        for item in items_lote:
            if not isinstance(item, dict):
                continue

            # [1, 2, 3] MAPEO INTELIGENTE Y TRADUCCIÓN DE VARIABLES VARIABLES
            fila_transformada = transformar_item_flexible(item, fecha_recepcion_api)

            # [4] CONTROL DE DUPLICIDAD
            clave_unica = f"{fila_transformada['fecha_registro']}_{fila_transformada['id_item']}_{fila_transformada['precio_valor']}_{fila_transformada['cantidad_volumen']}"
            
            if clave_unica in registro_duplicados:
                continue

            registro_duplicados.add(clave_unica)
            filas_finales.append(fila_transformada)

    return filas_finales

def transformar_item_flexible(item, fecha_recepcion_api):
    """
    Busca los datos dinámicamente usando sinónimos comunes para soportar cambios en la API.
    """
    # Intentar obtener el ID usando variantes comunes
    id_item = buscar_llave(item, ["id_componente", "id", "id_producto", "id_cliente", "codigo"])
    
    # Intentar obtener el Nombre
    nombre_item = buscar_llave(item, ["componente", "producto", "cliente", "nombre", "item"])
    
    # Intentar obtener la Fecha
    fecha_registro = buscar_llave(item, ["fecreg", "fecha", "timestamp", "fecha_registro"])
    
    # Intentar obtener la Zona
    zona_geografica = buscar_llave(item, ["zona", "region", "sucursal", "ubicacion", "forma_pago"])
    
    # Intentar obtener Valores Numéricos
    precio_valor = transformar_a_decimal(buscar_llave(item, ["precio", "valor", "costo"]))
    cantidad_volumen = transformar_a_entero(buscar_llave(item, ["cantidad", "volumen", "unidades", "monto"]))

    # [5] ENRIQUECIMIENTO: Cálculo derivado automático
    monto_total_calculado = None
    if precio_valor is not None and cantidad_volumen is not None:
        monto_total_calculado = round(precio_valor * cantidad_volumen, 2)

    # [5] ENRIQUECIMIENTO: Categorización dinámica por patrones de texto
    categoria_enriquecida = "General / No Clasificado"
    if nombre_item:
        texto = nombre_item.lower()
        if any(p in texto for p in ["ram", "disco", "ssd", "tarjeta", "procesador", "gpu", "cpu", "componente"]):
            categoria_enriquecida = "Componentes de Hardware"
        elif any(p in texto for p in ["monitor", "teclado", "mouse", "gabinete", "audifonos", "periferico"]):
            categoria_enriquecida = "Periféricos y Accesorios"

    # [1] VALIDACIÓN
    alertas = []
    if not nombre_item: alertas.append("Nombre no detectado")
    if precio_valor is None: alertas.append("Precio no numérico o vacío")

    estado_validacion = "OK" if not alertas else "OBSERVADO"

    return {
        "fecha_recepcion_api": fecha_recepcion_api,
        "fecha_registro": fecha_registro if fecha_registro else fecha_recepcion_api,
        "id_item": id_item if id_item else "S/I",
        "nombre_item": nombre_item if nombre_item else "Item No Identificado",
        "categoria_enriquecida": categoria_enriquecida,
        "precio_valor": precio_valor if precio_valor is not None else "",
        "cantidad_volumen": cantidad_volumen if cantidad_volumen is not None else "",
        "monto_total_calculado": monto_total_calculado if monto_total_calculado is not None else "",
        "zona_geografica": zona_geografica if zona_geografica else "Zona General",
        "estado_validacion": estado_validacion,
        "observaciones_calidad": "; ".join(alertas)
    }

def buscar_llave(diccionario, lista_sinonimos):
    """Busca dentro del objeto JSON cualquier propiedad que coincida con la lista de variantes."""
    for sinonimo in lista_sinonimos:
        if sinonimo in diccionario:
            return str(diccionario[sinonimo]).strip()
    return ""

def transformar_a_decimal(valor_crudo):
    if valor_crudo is None or valor_crudo == "": return None
    try:
        texto = str(valor_crudo).strip().replace("$", "").replace(" ", "")
        if "," in texto and "." in texto:
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", ".")
        return float(texto)
    except ValueError:
        return None

def transformar_a_entero(valor_crudo):
    if valor_crudo is None or valor_crudo == "": return None
    try:
        return int(float(str(valor_crudo).strip().replace(",", ".")))
    except ValueError:
        return None

if __name__ == "__main__":
    puerto_servidor = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=puerto_servidor)