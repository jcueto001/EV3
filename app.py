from flask import Flask, request, jsonify, Response
from datetime import datetime
import json
import os
import csv
import re
from io import StringIO

app = Flask(__name__)

# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

# Archivo JSONL donde se guardan los datos crudos recibidos
ARCHIVO_DATOS = os.path.join(os.getcwd(), "ventas_tiempo_real.jsonl")

# Columnas finales del CSV limpio para Power BI
COLUMNAS_CSV = [
    "fecha_recepcion_api",
    "fecha_registro",
    "id_registro",
    "producto",
    "categoria",
    "precio",
    "cantidad",
    "monto_total",
    "forma_pago",
    "genero",
    "estado_validacion",
    "observaciones_calidad"
]


# ============================================================
# ENDPOINT PRINCIPAL
# ============================================================

@app.route("/", methods=["GET"])
def inicio():
    return jsonify({
        "estado": "activo",
        "mensaje": "API Big Data en tiempo real funcionando correctamente",
        "endpoints": {
            "webhook_post": "/webhook",
            "datos_crudos": "/datos-crudos",
            "datos_limpios": "/datos-limpios",
            "csv_powerbi": "/descargar-csv",
            "resumen": "/resumen",
            "debug": "/debug"
        }
    }), 200


# ============================================================
# ENDPOINT DE INGESTA EN TIEMPO REAL
# ============================================================

@app.route("/webhook", methods=["POST", "GET", "HEAD"])
def recibir_datos():
    """
    Endpoint que recibe datos desde la plataforma Real Time.
    La plataforma debe enviar solicitudes POST con JSON.
    """

    # Permite verificar que el endpoint existe desde navegador
    if request.method in ["GET", "HEAD"]:
        return jsonify({
            "estado": "activo",
            "mensaje": "Webhook disponible. Para enviar datos debe usarse método POST."
        }), 200

    cuerpo_crudo = request.get_data(as_text=True)

    if not cuerpo_crudo or not cuerpo_crudo.strip():
        return jsonify({
            "estado": "rechazado",
            "error": "El cuerpo recibido está vacío"
        }), 400

    try:
        estructura_json = json.loads(cuerpo_crudo)
    except json.JSONDecodeError:
        return jsonify({
            "estado": "rechazado",
            "error": "El cuerpo recibido no es un JSON válido"
        }), 400

    eventos = normalizar_entrada_json(estructura_json)

    if not eventos:
        return jsonify({
            "estado": "rechazado",
            "error": "No se encontraron registros válidos en el JSON recibido"
        }), 400

    fecha_recepcion = datetime.now().astimezone().isoformat()

    registro_auditoria = {
        "fecha_recepcion": fecha_recepcion,
        "cantidad_registros_lote": len(eventos),
        "data": eventos
    }

    try:
        with open(ARCHIVO_DATOS, "a", encoding="utf-8") as archivo:
            archivo.write(json.dumps(registro_auditoria, ensure_ascii=False) + "\n")
    except OSError as error:
        print(f"Error al guardar datos crudos: {error}", flush=True)
        return jsonify({
            "estado": "error",
            "error": "No fue posible guardar los datos recibidos"
        }), 500

    print(
        f"POST recibido correctamente: {len(eventos)} registro(s) - {fecha_recepcion}",
        flush=True
    )

    return jsonify({
        "estado": "recibido",
        "mensaje": "Datos recibidos y almacenados correctamente",
        "fecha_recepcion": fecha_recepcion,
        "registros_lote_actual": len(eventos),
        "lotes_acumulados": contar_lotes_crudos()
    }), 200


# ============================================================
# ENDPOINT PARA VER DATOS CRUDOS
# ============================================================

@app.route("/datos-crudos", methods=["GET"])
def datos_crudos():
    registros = leer_registros_crudos()
    return jsonify({
        "capa": "Bronze / Datos crudos",
        "total_lotes": len(registros),
        "datos": registros
    }), 200


# ============================================================
# ENDPOINT PARA VER DATOS LIMPIOS
# ============================================================

@app.route("/datos-limpios", methods=["GET"])
def datos_limpios():
    filas = obtener_datos_procesados()
    return jsonify({
        "capa": "Silver-Gold / Datos limpios para análisis",
        "total_registros": len(filas),
        "datos": filas
    }), 200


# ============================================================
# ENDPOINT CSV PARA POWER BI
# ============================================================

@app.route("/descargar-csv", methods=["GET"])
def descargar_csv():
    filas = obtener_datos_procesados()

    salida = StringIO()
    escritor = csv.DictWriter(salida, fieldnames=COLUMNAS_CSV)
    escritor.writeheader()
    escritor.writerows(filas)

    return Response(
        salida.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": "attachment; filename=ventas_limpias_powerbi.csv"
        }
    )


# ============================================================
# ENDPOINT RESUMEN AGREGADO
# ============================================================

@app.route("/resumen", methods=["GET"])
def resumen():
    filas = obtener_datos_procesados()

    total_registros = len(filas)
    registros_ok = sum(1 for f in filas if f["estado_validacion"] == "OK")
    registros_observados = sum(1 for f in filas if f["estado_validacion"] == "OBSERVADO")

    cantidad_total = sum(
        f["cantidad"] for f in filas
        if isinstance(f["cantidad"], int)
    )

    monto_total = sum(
        f["monto_total"] for f in filas
        if isinstance(f["monto_total"], (int, float))
    )

    monto_por_producto = {}
    distribucion_forma_pago = {}
    registros_por_genero = {}

    for fila in filas:
        producto = fila["producto"] or "Sin producto"
        forma_pago = fila["forma_pago"] or "Sin forma de pago"
        genero = fila["genero"] or "Sin género"

        monto = fila["monto_total"] if isinstance(fila["monto_total"], (int, float)) else 0

        monto_por_producto[producto] = monto_por_producto.get(producto, 0) + monto
        distribucion_forma_pago[forma_pago] = distribucion_forma_pago.get(forma_pago, 0) + 1
        registros_por_genero[genero] = registros_por_genero.get(genero, 0) + 1

    return jsonify({
        "total_registros_recibidos": total_registros,
        "registros_ok": registros_ok,
        "registros_observados": registros_observados,
        "cantidad_total": cantidad_total,
        "monto_total": round(monto_total, 2),
        "monto_por_producto": monto_por_producto,
        "distribucion_forma_pago": distribucion_forma_pago,
        "registros_por_genero": registros_por_genero
    }), 200


# ============================================================
# ENDPOINT DEBUG / INFORMACIÓN TÉCNICA
# ============================================================

@app.route("/debug", methods=["GET"])
def debug():
    existe_archivo = os.path.exists(ARCHIVO_DATOS)

    tamaño_archivo = 0
    if existe_archivo:
        tamaño_archivo = os.path.getsize(ARCHIVO_DATOS)

    return jsonify({
        "estado_api": "activa",
        "archivo_datos": ARCHIVO_DATOS,
        "existe_archivo_jsonl": existe_archivo,
        "tamaño_archivo_bytes": tamaño_archivo,
        "lotes_crudos": contar_lotes_crudos(),
        "registros_limpios": len(obtener_datos_procesados()),
        "endpoints_disponibles": [
            "/",
            "/webhook",
            "/datos-crudos",
            "/datos-limpios",
            "/descargar-csv",
            "/resumen",
            "/debug"
        ]
    }), 200


# ============================================================
# FUNCIONES DE LECTURA Y PROCESAMIENTO
# ============================================================

def normalizar_entrada_json(estructura_json):
    """
    Permite recibir distintos formatos:
    - Un objeto JSON
    - Una lista de objetos
    - Un objeto con clave data
    - Un objeto con clave registros
    - Un objeto con clave ventas
    """

    if isinstance(estructura_json, list):
        return estructura_json

    if isinstance(estructura_json, dict):
        for clave in ["data", "registros", "ventas", "items", "payload"]:
            if clave in estructura_json:
                contenido = estructura_json[clave]
                if isinstance(contenido, list):
                    return contenido
                if isinstance(contenido, dict):
                    return [contenido]

        return [estructura_json]

    return []


def leer_registros_crudos():
    if not os.path.exists(ARCHIVO_DATOS):
        return []

    registros = []

    try:
        with open(ARCHIVO_DATOS, "r", encoding="utf-8") as archivo:
            for linea in archivo:
                if linea.strip():
                    registros.append(json.loads(linea))
    except Exception as error:
        print(f"Error leyendo archivo JSONL: {error}", flush=True)

    return registros


def contar_lotes_crudos():
    return len(leer_registros_crudos())


def obtener_datos_procesados():
    registros_crudos = leer_registros_crudos()
    filas_finales = []
    claves_duplicadas = set()

    for lote in registros_crudos:
        fecha_recepcion_api = lote.get("fecha_recepcion", "")
        items = lote.get("data", [])

        for item in items:
            if not isinstance(item, dict):
                continue

            fila = transformar_registro(item, fecha_recepcion_api)

            clave_compuesta = crear_clave_duplicidad(fila)

            if clave_compuesta in claves_duplicadas:
                continue

            claves_duplicadas.add(clave_compuesta)
            filas_finales.append(fila)

    return filas_finales


def transformar_registro(item, fecha_recepcion_api):
    """
    Limpia, normaliza, transforma, valida y enriquece cada registro.
    """

    item_normalizado = normalizar_claves(item)

    fecha_registro = buscar_valor(item_normalizado, [
        "fecha",
        "fecha_registro",
        "timestamp",
        "fecreg",
        "fecha_venta"
    ])

    id_registro = buscar_valor(item_normalizado, [
        "id",
        "id_registro",
        "id_venta",
        "codigo",
        "codigo_venta",
        "id_transaccion"
    ])

    producto = buscar_valor(item_normalizado, [
        "producto",
        "nombre_producto",
        "item",
        "nombre_item",
        "descripcion",
        "articulo"
    ])

    precio = convertir_decimal(buscar_valor(item_normalizado, [
        "precio",
        "precio_unitario",
        "valor",
        "costo"
    ]))

    cantidad = convertir_entero(buscar_valor(item_normalizado, [
        "cantidad",
        "unidades",
        "qty",
        "volumen"
    ]))

    monto_recibido = convertir_decimal(buscar_valor(item_normalizado, [
        "monto",
        "monto_total",
        "total",
        "valor_total"
    ]))

    forma_pago = buscar_valor(item_normalizado, [
        "forma_pago",
        "medio_pago",
        "metodo_pago",
        "pago"
    ])

    genero = buscar_valor(item_normalizado, [
        "genero",
        "sexo",
        "gender"
    ])

    producto_limpio = limpiar_texto(producto)
    forma_pago_limpia = limpiar_texto(forma_pago)
    genero_limpio = limpiar_texto(genero)

    if precio is not None and cantidad is not None:
        monto_total = round(precio * cantidad, 2)
    elif monto_recibido is not None:
        monto_total = monto_recibido
    else:
        monto_total = ""

    categoria = clasificar_categoria(producto_limpio)

    observaciones = []

    if not producto_limpio:
        observaciones.append("Producto no informado")

    if precio is None and monto_recibido is None:
        observaciones.append("Precio o monto no numérico")

    if cantidad is None:
        observaciones.append("Cantidad no numérica o vacía")

    if precio is not None and precio < 0:
        observaciones.append("Precio negativo")

    if cantidad is not None and cantidad <= 0:
        observaciones.append("Cantidad menor o igual a cero")

    estado_validacion = "OK" if not observaciones else "OBSERVADO"

    return {
        "fecha_recepcion_api": fecha_recepcion_api,
        "fecha_registro": fecha_registro if fecha_registro else fecha_recepcion_api,
        "id_registro": id_registro if id_registro else "S/I",
        "producto": producto_limpio if producto_limpio else "Sin producto",
        "categoria": categoria,
        "precio": precio if precio is not None else "",
        "cantidad": cantidad if cantidad is not None else "",
        "monto_total": monto_total,
        "forma_pago": forma_pago_limpia if forma_pago_limpia else "Sin forma de pago",
        "genero": genero_limpio if genero_limpio else "Sin género",
        "estado_validacion": estado_validacion,
        "observaciones_calidad": "; ".join(observaciones)
    }


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def normalizar_claves(diccionario):
    """
    Convierte nombres de columnas a minúscula, sin espacios raros.
    """

    nuevo = {}

    for clave, valor in diccionario.items():
        clave_limpia = str(clave).strip().lower()
        clave_limpia = clave_limpia.replace(" ", "_")
        clave_limpia = clave_limpia.replace("-", "_")
        clave_limpia = clave_limpia.replace(".", "_")
        nuevo[clave_limpia] = valor

    return nuevo


def buscar_valor(diccionario, posibles_claves):
    for clave in posibles_claves:
        if clave in diccionario:
            valor = diccionario[clave]
            if valor is not None:
                return str(valor).strip()
    return ""


def limpiar_texto(valor):
    if valor is None:
        return ""

    texto = str(valor).strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto.title()


def convertir_decimal(valor):
    if valor is None or valor == "":
        return None

    try:
        texto = str(valor).strip()
        texto = texto.replace("$", "")
        texto = texto.replace("CLP", "")
        texto = texto.replace("clp", "")
        texto = texto.replace(" ", "")

        # Caso chileno: 1.500,50
        if "," in texto and "." in texto:
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", ".")

        return float(texto)
    except ValueError:
        return None


def convertir_entero(valor):
    if valor is None or valor == "":
        return None

    try:
        texto = str(valor).strip().replace(",", ".")
        return int(float(texto))
    except ValueError:
        return None


def clasificar_categoria(producto):
    if not producto:
        return "No clasificado"

    texto = producto.lower()

    if any(palabra in texto for palabra in ["notebook", "pc", "computador", "monitor", "teclado", "mouse"]):
        return "Tecnología"

    if any(palabra in texto for palabra in ["polera", "pantalon", "zapato", "zapatilla", "chaqueta"]):
        return "Vestuario"

    if any(palabra in texto for palabra in ["pan", "bebida", "arroz", "leche", "galleta", "comida"]):
        return "Alimentos"

    if any(palabra in texto for palabra in ["mesa", "silla", "cama", "mueble"]):
        return "Hogar"

    return "General"


def crear_clave_duplicidad(fila):
    """
    Controla duplicados mediante clave compuesta.
    """

    return (
        f"{fila['fecha_registro']}_"
        f"{fila['id_registro']}_"
        f"{fila['producto']}_"
        f"{fila['monto_total']}"
    )


# ============================================================
# EJECUCIÓN LOCAL / RENDER
# ============================================================

if __name__ == "__main__":
    puerto = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=puerto)