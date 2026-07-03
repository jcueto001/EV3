from flask import Flask, request, jsonify, Response
from datetime import datetime
import json
import os
import csv
from io import StringIO

app = Flask(__name__)

# Archivo persistente en el disco del servidor para almacenar la ingesta cruda
DATA_FILE = os.path.join(os.getcwd(), "subastas_realtime.jsonl")

# Columnas definitivas para la Capa de Consumo (CSV / Dashboard) que pide el informe
COLUMNAS_CSV = [
    "fecha_recepcion_api",
    "fecha_subasta",
    "id_componente",
    "componente",
    "categoria",
    "precio_subastado",
    "cantidad_lote",
    "monto_total_transaccion",
    "zona_geografica",
    "estado_validacion",
    "observaciones"
]

# Campos esperados del endpoint de Duoc UC para la subasta de componentes
CAMPOS_REFERENCIA_SUBASTA = {
    "id_componente",
    "componente",
    "fecreg",         # Fecha de registro en el origen
    "precio",         # Precio fluctuante del componente
    "cantidad",       # Volumen del lote subastado
    "zona"            # Zona/Horario del servicio
}

@app.route("/", methods=["GET"])
def inicio():
    return "Pipeline Big Data Duoc UC (AVY1101) - Activo", 200

@app.route("/webhook", methods=["POST"])
def recibir_datos():
    """
    IL 3.1: Proceso de ingesta en línea utilizando la API institucional.
    Captura las fluctuaciones en tiempo real enviadas por POST.
    """
    cuerpo = request.get_data(as_text=True)

    if not cuerpo or not cuerpo.strip():
        return jsonify({
            "estado": "rechazado",
            "error": "Cuerpo vacío",
            "detalle": "La solicitud de la subasta no contiene datos."
        }), 400

    try:
        payload = json.loads(cuerpo)
    except json.JSONDecodeError:
        return jsonify({
            "estado": "rechazado",
            "error": "JSON inválido",
            "detalle": "Estructura de datos corrupta o no parseable."
        }), 400

    # Extraer eventos adaptados al nuevo dominio de hardware
    eventos, error_estructura = extraer_eventos_entrada(payload)

    if error_estructura:
        return jsonify({
            "estado": "rechazado",
            "error": "Estructura inválida",
            "detalle": error_estructura
        }), 400

    fecha_recepcion = datetime.now().astimezone().isoformat()

    registro = {
        "fecha_recepcion": fecha_recepcion,
        "data": eventos
    }

    print(f"Streaming Duoc UC: {len(eventos)} componente(s) subastado(s) - {fecha_recepcion}", flush=True)

    try:
        with open(DATA_FILE, "a", encoding="utf-8") as archivo:
            archivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
    except OSError as error:
        print("Error de almacenamiento crítico:", error, flush=True)
        return jsonify({
            "estado": "error",
            "error": "Control de errores de infraestructura: No se pudo escribir en el Data Lake."
        }), 500

    return jsonify({
        "estado": "recibido",
        "mensaje": "Datos de subasta procesados correctamente",
        "fecha_recepcion": fecha_recepcion,
        "registros_recibidos": len(eventos),
        "total_acumulado_crudo": contar_registros_crudos()
    }), 200

@app.route("/ver-datos", methods=["GET"])
def ver_datos():
    datos = leer_registros_crudos()
    return jsonify({
        "archivo_origen": os.path.basename(DATA_FILE),
        "total_registros_crudos": len(datos),
        "datos": datos
    }), 200

@app.route("/datos-limpios", methods=["GET"])
def datos_limpios():
    """
    Muestra los datos aplicando las transformaciones de la pauta.
    """
    filas = obtener_datos_limpios()
    return jsonify({
        "mensaje": "Capa Silver/Gold generada",
        "total_registros_limpios": len(filas),
        "datos": filas
    }), 200

@app.route("/descargar-csv", methods=["GET"])
def descargar_csv():
    """
    Exportación de datos limpios para la carga histórica / Capa de consumo.
    """
    filas = obtener_datos_limpios()
    salida = StringIO()
    writer = csv.DictWriter(salida, fieldnames=COLUMNAS_CSV)
    writer.writeheader()
    writer.writerows(filas)
    
    return Response(
        salida.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=subastas_clean_streaming.csv"}
    )

@app.route("/resumen", methods=["GET"])
def resumen():
    """
    IL 3.3: Sintetiza métricas clave para demostrar tendencias y patrones en el Panel de Control.
    """
    filas = obtener_datos_limpios()

    total_monto = sum(f["monto_total_transaccion"] for f in filas if isinstance(f["monto_total_transaccion"], (int, float)))
    total_unidades = sum(f["cantidad_lote"] for f in filas if isinstance(f["cantidad_lote"], int))

    componentes_metricas = {}
    zonas_metricas = {}

    for fila in filas:
        comp = fila["componente"] or "Desconocido"
        zona = fila["zona_geografica"] or "Zonas Sin Registrar"
        
        componentes_metricas[comp] = componentes_metricas.get(comp, 0) + 1
        zonas_metricas[zona] = zonas_metricas.get(zona, 0) + 1

    return jsonify({
        "indicador_logro": "IL 3.3 - Panel Control",
        "total_registros_procesados": len(filas),
        "volumen_total_unidades": total_unidades,
        "valor_total_mercado_subastado": round(total_monto, 2),
        "frecuencia_por_componente": componentes_metricas,
        "frecuencia_por_zona_servicio": zonas_metricas
    }), 200


# =====================================================================
# SECCIÓN TÉCNICA: LOGICA DE PROCESAMIENTO, ENRIQUECIMIENTO Y LIMPIEZA (IL 3.2)
# =====================================================================

def extraer_eventos_entrada(payload):
    if isinstance(payload, list):
        eventos = payload
    elif isinstance(payload, dict):
        if "data" in payload:
            contenido = payload["data"]
            eventos = contenido if isinstance(contenido, list) else [contenido]
        elif es_estructura_subasta_valida(payload):
            eventos = [payload]
        else:
            return None, "Estructura JSON desconocida para el negocio de componentes."
    else:
        return None, "El cuerpo debe ser un objeto o lista JSON."

    if not eventos:
        return None, "Lote vacío."

    for idx, evento in enumerate(eventos, start=1):
        if not isinstance(evento, dict) or not es_estructura_subasta_valida(evento):
            return None, f"El registro {idx} no cumple con los indicadores de referencia mínimos."

    return eventos, None

def es_estructura_subasta_valida(item):
    if not isinstance(item, dict):
        return False
    return any(campo in item for campo in CAMPOS_REFERENCIA_SUBASTA)

def leer_registros_crudos():
    if not os.path.exists(DATA_FILE):
        return []
    datos = []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as archivo:
            for linea in archivo:
                if linea.strip():
                    datos.append(json.loads(linea))
    except OSError:
        print("Error leyendo almacenamiento crudo.")
    return datos

def contar_registros_crudos():
    return len(leer_registros_crudos())

def obtener_datos_limpios():
    """
    IL 3.2: Aplica de forma estricta los requerimientos de la rúbrica docente.
    """
    registros = leer_registros_crudos()
    filas = []
    duplicados = set() # Estructura en memoria para controlar duplicidad en tiempo real

    for registro in registros:
        fecha_recepcion = registro.get("fecha_recepcion", "")
        items = registro.get("data", [])

        for item in items:
            if not es_estructura_subasta_valida(item):
                continue

            # 1. NORMALIZACIÓN, VALIDACIÓN Y ENRIQUECIMIENTO
            fila = transformar_y_enriquecer_item(item, fecha_recepcion)

            # 2. CONTROL DE DUPLICIDAD (Deduplicación en la capa de consumo)
            clave_duplicado = f"{fila['fecha_subasta']}_{fila['id_componente']}_{fila['precio_subastado']}_{fila['cantidad_lote']}"
            
            if clave_duplicado in duplicados:
                continue  # Evita duplicidad de datos Real-Time / Streaming

            duplicados.add(clave_duplicado)
            filas.append(fila)

    # Ordenamiento cronológico lógico para análisis de tendencias
    filas.sort(key=lambda f: (f["fecha_subasta"] or f["fecha_recepcion_api"]))
    return filas

def transformar_y_enriquecer_item(item, fecha_recepcion):
    """
    Implementa Limpieza, Validación y Enriquecimiento de Datos.
    """
    precio = convertir_float(item.get("precio"))
    cantidad = convertir_int(item.get("cantidad"))
    
    # ENRIQUECIMIENTO DE DATOS: Cálculo derivado para responder preguntas de negocio
    monto_total = round(precio * cantidad, 2) if (precio is not None and cantidad is not None) else None

    componente = limpiar_texto(item.get("componente"))
    zona = limpiar_texto(item.get("zona"))
    id_comp = limpiar_texto(item.get("id_componente"))

    # ENRIQUECIMIENTO ADICIONAL: Regla de negocio para categorizar componentes dinámicamente
    categoria = "Sin Categoría"
    if componente:
        comp_lower = componente.lower()
        if any(x in comp_lower for x in ["ram", "disco", "ssd", "tarjeta", "procesador"]):
            categoria = "Hardware Interno"
        elif any(x in comp_lower for x in ["monitor", "teclado", "mouse", "gabinete"]):
            categoria = "Periféricos"

    observaciones = []
    if not componente: observaciones.append("Nombre de componente faltante")
    if precio is None: observaciones.append("Precio nulo o inválido")
    if cantidad is None: observaciones.append("Cantidad vacía")

    estado_validacion = "OK" if not observaciones else "OBSERVADO"

    return {
        "fecha_recepcion_api": fecha_recepcion,
        "fecha_subasta": limpiar_texto(item.get("fecreg")),
        "id_componente": id_comp,
        "componente": componente,
        "categoria": categoria,
        "precio_subastado": precio if precio is not None else "",
        "cantidad_lote": cantidad if cantidad is not None else "",
        "monto_total_transaccion": monto_total if monto_total is not None else "",
        "zona_geografica": zona if zona else "Zona General",
        "estado_validacion": estado_validacion,
        "observaciones": "; ".join(observaciones)
    }

def limpiar_texto(valor):
    return str(valor).strip() if valor is not None else ""

def convertir_float(valor):
    if valor is None or valor == "": return None
    try:
        texto = str(valor).strip().replace("$", "").replace(" ", "")
        if "," in texto and "." in texto:
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", ".")
        return float(texto)
    except ValueError:
        return None

def convertir_int(valor):
    if valor is None or valor == "": return None
    try:
        return int(float(str(valor).strip().replace(",", ".")))
    except ValueError:
        return None

if __name__ == "__main__":
    # Lee dinámicamente el puerto asignado por Render
    puerto = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=puerto)
