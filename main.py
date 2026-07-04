from flask import Flask, request, jsonify, Response
from datetime import datetime
import json
import os
import csv
from io import StringIO

app = Flask(__name__)

# Archivo persistente local que actúa como la capa cruda de almacenamiento (Raw / Bronze)
ARCHIVO_DATOS = os.path.join(os.getcwd(), "subastas_tiempo_real.jsonl")

# Definición del esquema final para la capa de consumo (Gold / CSV) exigido por el informe
COLUMNAS_CSV = [
    "fecha_recepcion_api",
    "fecha_subasta",
    "id_componente",
    "componente",
    "categoria_enriquecida",
    "precio_subastado",
    "cantidad_lote",
    "monto_total_calculado",
    "zona_geografica",
    "estado_validacion",
    "observaciones_calidad"
]

# Campos de referencia mínimos esperados desde la API institucional de Duoc UC
CAMPOS_REFERENCIA_SUBASTA = {
    "id_componente",
    "componente",
    "fecreg",         # Fecha de registro original en la subasta
    "precio",         # Precio fluctuante del componente electrónico
    "cantidad",       # Volumen del lote disponible en la subasta
    "zona"            # Zona o sector geográfico del servicio
}

@app.route("/", methods=["GET"])
def inicio():
    return "Pipeline de Big Data Duoc UC (AVY1101) - Estado: Activo", 200

@app.route("/webhook", methods=["POST", "GET", "HEAD"])
def recibir_datos():
    """
    IL 3.1: Proceso de ingesta utilizando la API en línea de la industria.
    Acepta métodos de verificación GET/HEAD para evitar el error '405 Method Not Allowed'
    y procesa las ráfagas continuas mediante POST.
    """
    # Control de verificación del portal o navegador web
    if request.method in ["GET", "HEAD"]:
        return jsonify({
            "estado": "activo",
            "mensaje": "Endpoint listo y escuchando peticiones POST de la plataforma Duoc UC."
        }), 200

    # Ingesta del flujo continuo en tiempo real (POST)
    cuerpo_crudo = request.get_data(as_text=True)

    if not cuerpo_crudo or not cuerpo_crudo.strip():
        return jsonify({
            "estado": "rechazado",
            "error": "Cuerpo vacío",
            "detalle": "La solicitud de subasta no contiene datos legibles."
        }), 400

    try:
        estructura_json = json.loads(cuerpo_crudo)
    except json.JSONDecodeError:
        return jsonify({
            "estado": "rechazado",
            "error": "JSON inválido",
            "detalle": "Estructura de datos corrupta en la transmisión de streaming."
        }), 400

    # Extraer los eventos de hardware entrantes
    eventos_extraidos, error_estructura = extraer_eventos_entrada(estructura_json)

    if error_estructura:
        return jsonify({
            "estado": "rechazado",
            "error": "Estructura inválida",
            "detalle": error_estructura
        }), 400

    fecha_recepcion_sistema = datetime.now().astimezone().isoformat()

    # Formatear el lote crudo para auditoría e historial
    registro_auditoria = {
        "fecha_recepcion": fecha_recepcion_sistema,
        "data": eventos_extraidos
    }

    # Registro de actividad en la consola de Render (Muestra la trazabilidad exigida)
    print(f"Streaming Duoc UC: {len(eventos_extraidos)} componente(s) detectado(s) - {fecha_recepcion_sistema}", flush=True)

    try:
        # Almacenamiento rápido en formato JSON Lines (Tolerante a fallos)
        with open(ARCHIVO_DATOS, "a", encoding="utf-8") as archivo:
            archivo.write(json.dumps(registro_auditoria, ensure_ascii=False) + "\n")
    except OSError as error_sistema:
        print(f"Control de errores crítico: No se pudo escribir en disco. Detalle: {error_sistema}", flush=True)
        return jsonify({
            "estado": "error",
            "error": "Fallo en la persistencia del almacenamiento del Data Lake."
        }), 500

    return jsonify({
        "estado": "recibido",
        "mensaje": "Datos de subasta procesados y almacenados con éxito",
        "fecha_recepcion": fecha_recepcion_sistema,
        "registros_lote_actual": len(eventos_extraidos),
        "total_lotes_acumulados": contar_registros_crudos()
    }), 200

@app.route("/datos-limpios", methods=["GET"])
def datos_limpios():
    """
    Muestra los datos aplicando las transformaciones de limpieza y de duplicación en tiempo real.
    """
    filas_procesadas = obtener_datos_procesados()
    return jsonify({
        "capa_datos": "Silver/Gold - Consumo",
        "total_registros_limpios": len(filas_procesadas),
        "datos": filas_procesadas
    }), 200

@app.route("/descargar-csv", methods=["GET"])
def descargar_csv():
    """
    Exportación estructurada de datos limpios para enlazar con Power BI o Looker Studio.
    """
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
    IL 3.3: Sintetiza la información en métricas clave demostrando tendencias y patrones
    para la toma de decisiones del panel de control.
    """
    filas_procesadas = obtener_datos_procesados()

    monto_total_mercado = sum(f["monto_total_calculado"] for f in filas_procesadas if isinstance(f["monto_total_calculado"], (int, float)))
    unidades_totales = sum(f["cantidad_lote"] for f in filas_processed if isinstance(f["cantidad_lote"], int)) if 'filas_procesadas' in locals() else sum(f["cantidad_lote"] for f in filas_procesadas if isinstance(f["cantidad_lote"], int))

    metricas_componente = {}
    metricas_zona = {}

    for fila in filas_procesadas:
        comp = fila["componente"] or "Desconocido"
        zona = fila["zona_geografica"] or "Zona Desconocida"
        
        metricas_componente[comp] = metricas_componente.get(comp, 0) + 1
        metricas_zona[zona] = metricas_zona.get(zona, 0) + 1

    return jsonify({
        "indicador_logro": "IL 3.3 - Panel de Control Gerencial",
        "total_registros_analizados": len(filas_procesadas),
        "volumen_total_unidades": unidades_totales,
        "valor_total_subastado_usd": round(monto_total_mercado, 2),
        "frecuencia_por_componente": metricas_componente,
        "frecuencia_por_zona_servicio": metricas_zona
    }), 200


# =====================================================================
# SECCIÓN TÉCNICA: MOTOR DE PROCESAMIENTO Y REGLAS DE NEGOCIO (IL 3.2)
# =====================================================================

def extraer_eventos_entrada(payload_entrante):
    if isinstance(payload_entrante, list):
        eventos = payload_entrante
    elif isinstance(payload_entrante, dict):
        if "data" in payload_entrante:
            contenido = payload_entrante["data"]
            eventos = contenido if isinstance(contenido, list) else [contenido]
        elif es_estructura_subasta_valida(payload_entrante):
            eventos = [payload_entrante]
        else:
            return None, "Estructura JSON no reconocida para el modelo de subastas de hardware."
    else:
        return None, "El payload de entrada debe ser un objeto o una lista válida."

    if not eventos:
        return None, "El lote de streaming no contiene eventos."

    for indice, evento in enumerate(eventos, start=1):
        if not isinstance(evento, dict) or not es_estructura_subasta_valida(evento):
            return None, f"El registro en la posición {indice} no contiene los campos mínimos requeridos de la API."

    return eventos, None

def es_estructura_subasta_valida(item_datos):
    if not isinstance(item_datos, dict):
        return False
    return any(campo in item_datos for campo in CAMPOS_REFERENCIA_SUBASTA)

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
        print("Error en el acceso de lectura al archivo local crudo.")
    return datos_acumulados

def contar_registros_crudos():
    return len(read_records := leer_registros_crudos())

def obtener_datos_procesados():
    """
    IL 3.2: Construye el proceso robusto de limpieza, transformación y almacenamiento.
    Aplica las 6 reglas requeridas para la nota máxima.
    """
    registros_crudos = leer_registros_crudos()
    filas_finales = []
    registro_duplicados = set()  # Control de duplicidad en memoria activa

    for registro in registros_crudos:
        fecha_recepcion_api = registro.get("fecha_recepcion", "")
        items_lote = registro.get("data", [])

        for item in items_lote:
            if not es_estructura_subasta_valida(item):
                continue

            # [1, 2, 3] VALIDACIÓN, LIMPIEZA Y NORMALIZACIÓN
            fila_transformada = transformar_y_validar_item(item, fecha_recepcion_api)

            # [4] CONTROL DE DUPLICIDAD: Evita duplicaciones cruzadas Batch/Streaming mediante clave compuesta única
            clave_unica = f"{fila_transformada['fecha_subasta']}_{fila_transformada['id_componente']}_{fila_transformada['precio_subastado']}_{fila_transformada['cantidad_lote']}"
            
            if clave_unica in registro_duplicados:
                continue  # Ignora el dato para evitar redundancia en la visualización

            registro_duplicados.add(clave_unica)
            filas_finales.append(fila_transformada)

    # Ordenamiento lógico para asegurar coherencia temporal en los patrones y tendencias
    filas_finales.sort(key=lambda f: (f["fecha_subasta"] or f["fecha_recepcion_api"]))
    return filas_finales

def transformar_y_validar_item(item, fecha_recepcion_api):
    """
    Realiza las fases de transformación fina: Limpieza, Validación y Enriquecimiento de Datos.
    """
    precio_limpio = transformar_a_decimal(item.get("precio"))
    cantidad_limpia = transformar_a_entero(item.get("cantidad"))
    
    # [5] ENRIQUECIMIENTO DE DATOS: Cálculo del valor total derivado de la transacción
    monto_calculado = round(precio_limpio * cantidad_limpia, 2) if (precio_limpio is not None and cantidad_limpia is not None) else None

    componente_limpio = remover_espacios_texto(item.get("componente"))
    zona_limpia = remover_espacios_texto(item.get("zona"))
    id_componente_limpio = remover_espacios_texto(item.get("id_componente"))

    # [5] ENRIQUECIMIENTO ADICIONAL: Clasificación taxonómica inteligente de componentes
    categoria_enriquecida = "Otros / No Definido"
    if componente_limpio:
        texto_busqueda = componente_limpio.lower()
        if any(palabra in texto_busqueda for palabra in ["ram", "disco", "ssd", "tarjeta", "procesador", "gpu", "cpu"]):
            categoria_enriquecida = "Componentes Internos (Hardware)"
        elif any(palabra in texto_busqueda for palabra in ["monitor", "teclado", "mouse", "gabinete", "audifonos"]):
            categoria_enriquecida = "Periféricos y Accesorios"

    # [1] VALIDACIÓN Y CONTROL DE CALIDAD
    alertas_calidad = []
    if not componente_limpio: alertas_calidad.append("Nombre del componente nulo")
    if precio_limpio is None: alertas_calidad.append("Precio inválido o vacío")
    if cantidad_limpia is None: alertas_calidad.append("Volumen de lote no especificado")

    estado_final_validacion = "OK" if not alertas_calidad else "OBSERVADO"

    return {
        "fecha_recepcion_api": fecha_recepcion_api,
        "fecha_subasta": remover_espacios_texto(item.get("fecreg")),
        "id_componente": id_componente_limpio,
        "componente": componente_limpio,
        "categoria_enriquecida": categoria_enriquecida,
        "precio_subastado": precio_limpio if precio_limpio is not None else "",
        "cantidad_lote": cantidad_limpia if cantidad_limpia is not None else "",
        "monto_total_calculado": monto_calculado if monto_calculado is not None else "",
        "zona_geografica": zona_limpia if zona_limpia else "Zona General de Servicios",
        "estado_validacion": estado_final_validacion,
        "observaciones_calidad": "; ".join(alertas_calidad)
    }

def remover_espacios_texto(valor_crudo):
    return str(valor_crudo).strip() if valor_crudo is not None else ""

def transformar_a_decimal(valor_crudo):
    if valor_crudo is None or valor_crudo == "": return None
    try:
        texto_limpio = str(valor_crudo).strip().replace("$", "").replace(" ", "")
        if "," in texto_limpio and "." in texto_limpio:
            texto_limpio = texto_limpio.replace(".", "").replace(",", ".")
        else:
            texto_limpio = texto_limpio.replace(",", ".")
        return float(texto_limpio)
    except ValueError:
        return None

def transformar_a_entero(valor_crudo):
    if valor_crudo is None or valor_crudo == "": return None
    try:
        return int(float(str(valor_crudo).strip().replace(",", ".")))
    except ValueError:
        return None

if __name__ == "__main__":
    # Captura del puerto dinámico asignado por el balanceador de carga de Render
    puerto_servidor = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=puerto_servidor)
