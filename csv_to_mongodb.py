"""
csv_to_mongodb.py
=================
Transforma el archivo CSV de registros aduaneros a un archivo JSON
estructurado y listo para importar a una colección de MongoDB.

Uso:
    python csv_to_mongodb.py                          # usa rutas por defecto
    python csv_to_mongodb.py input.csv output.json    # rutas personalizadas

Importar a MongoDB luego con:
    mongoimport --uri "mongodb+srv://<user>:<pass>@<cluster>/aduana" \
                --collection declaraciones \
                --file registros_aduana_mongodb.json \
                --jsonArray
"""

import csv
import json
import sys
import os
from datetime import datetime, date

# MongoDB client
try:
    from pymongo import MongoClient, errors
    from pymongo.errors import BulkWriteError, ConnectionFailure, ServerSelectionTimeoutError
    from pymongo.operations import UpdateOne
except Exception:  # pragma: no cover - optional dependency
    MongoClient = None
    errors = None
    BulkWriteError = None
    ConnectionFailure = None
    ServerSelectionTimeoutError = None
    UpdateOne = None

# ──────────────────────────────────────────────
# Configuración de rutas
# ──────────────────────────────────────────────
DEFAULT_INPUT  = "registros_aduana_sinteticos.csv"
DEFAULT_OUTPUT = "registros_aduana_mongodb.json"

INPUT_FILE  = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
OUTPUT_FILE = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT


# ──────────────────────────────────────────────
# Helpers de conversión de tipos
# ──────────────────────────────────────────────

def to_float(value: str, default=None):
    """Convierte string a float; retorna default si está vacío o es inválido."""
    try:
        return float(value.strip()) if value.strip() else default
    except (ValueError, AttributeError):
        return default

def to_int(value: str, default=None):
    """Convierte string a int; retorna default si está vacío o es inválido."""
    try:
        return int(value.strip()) if value.strip() else default
    except (ValueError, AttributeError):
        return default

def to_date(value: str, fmt="%Y-%m-%d"):
    """
    Convierte string a objeto date de Python.
    En la salida JSON se serializa como ISO-8601 string (YYYY-MM-DD).
    Si necesitas ISODate de MongoDB, usa mongoimport con --type json.
    """
    try:
        return datetime.strptime(value.strip(), fmt).strftime(fmt) if value.strip() else None
    except (ValueError, AttributeError):
        return None

def parse_containers(value: str) -> list[str]:
    """Convierte 'MSCU123|TCKU456' → ['MSCU123', 'TCKU456']."""
    if not value or not value.strip():
        return []
    return [c.strip() for c in value.split("|") if c.strip()]

def parse_items(value: str) -> list[dict]:
    """
    Convierte el campo items_detalle al formato:
        '1:8471.30.00:Computadores:5:12500.00USD | 2:...'
    en una lista de subdocumentos MongoDB.
    """
    items = []
    if not value or not value.strip():
        return items

    for segment in value.split("|"):
        segment = segment.strip()
        if not segment:
            continue
        parts = segment.split(":")
        if len(parts) < 5:
            continue
        try:
            valor_str = parts[4].replace("USD", "").replace("EUR", "").strip()
            items.append({
                "item_numero"       : to_int(parts[0]),
                "codigo_arancelario": parts[1].strip(),
                "descripcion"       : parts[2].strip(),
                "cantidad"          : to_int(parts[3]),
                "valor_usd"         : to_float(valor_str),
            })
        except (IndexError, ValueError):
            continue
    return items


# ──────────────────────────────────────────────
# Función principal de transformación
# ──────────────────────────────────────────────

def transform_row(row: dict) -> dict:
    """
    Convierte una fila plana del CSV al documento MongoDB anidado.
    Estructura final:
        {
          _id, numero_declaracion, fecha_declaracion,
          importador     { nombre, tax_id, pais },
          exportador     { pais },
          embarque       { naviera, nave, bl, puerto_origen,
                           fecha_embarque, fecha_arribo, dias_transito,
                           via_transporte },
          manifiesto     { numero, contenedores, tipo_contenedor,
                           num_contenedores, peso_bruto_kg,
                           volumen_m3, bultos },
          mercaderias    [ { item_numero, codigo_arancelario,
                             descripcion, cantidad, valor_usd } ],
          aduana         { codigo_pais, nombre_aduana, canal, estado,
                           fecha_liquidacion },
          valores        { fob_usd, flete_usd, seguro_usd, cif_usd,
                           tasa_advalorem_pct, derechos_advalorem,
                           iva, total_tributos, moneda },
          metadata       { num_items, creado_en }
        }
    """
    doc = {
        # Identificador único → _id de MongoDB
        "_id": row.get("numero_declaracion", "").strip(),
        "numero_declaracion": row.get("numero_declaracion", "").strip(),
        "fecha_declaracion" : to_date(row.get("fecha_declaracion", "")),

        # ── Importador ──────────────────────────────
        "importador": {
            "nombre" : row.get("importador_nombre", "").strip(),
            "tax_id" : row.get("importador_tax_id", "").strip(),
            "pais"   : row.get("pais_importacion", "").strip(),
        },

        # ── Exportador ──────────────────────────────
        "exportador": {
            "pais": row.get("exportador_pais", "").strip(),
        },

        # ── Embarque ────────────────────────────────
        "embarque": {
            "naviera"        : row.get("naviera", "").strip(),
            "nombre_nave"    : row.get("nombre_nave", "").strip(),
            "numero_bl"      : row.get("numero_bl", "").strip(),
            "puerto_origen"  : row.get("puerto_origen", "").strip(),
            "fecha_embarque" : to_date(row.get("fecha_embarque", "")),
            "fecha_arribo"   : to_date(row.get("fecha_arribo", "")),
            "dias_transito"  : to_int(row.get("dias_transito", "")),
            "via_transporte" : row.get("via_transporte", "").strip(),
        },

        # ── Manifiesto ──────────────────────────────
        "manifiesto": {
            "numero"          : row.get("numero_manifiesto", "").strip(),
            "contenedores"    : parse_containers(row.get("contenedores", "")),
            "tipo_contenedor" : row.get("tipo_contenedor", "").strip(),
            "num_contenedores": to_int(row.get("num_contenedores", "")),
            "peso_bruto_kg"   : to_float(row.get("peso_bruto_kg", "")),
            "volumen_m3"      : to_float(row.get("volumen_m3", "")),
            "bultos"          : to_int(row.get("bultos", "")),
        },

        # ── Mercaderías (array de subdocumentos) ────
        "mercaderias": parse_items(row.get("items_detalle", "")),

        # ── Datos de aduana ─────────────────────────
        "aduana": {
            "codigo_pais"      : row.get("pais_importacion", "").strip(),
            "nombre_aduana"    : row.get("aduana", "").strip(),
            "canal"            : row.get("canal", "").strip(),
            "estado"           : row.get("estado", "").strip(),
            "fecha_liquidacion": to_date(row.get("fecha_liquidacion", "")),
        },

        # ── Valores y tributos ──────────────────────
        "valores": {
            "fob_usd"           : to_float(row.get("valor_fob_usd", "")),
            "flete_usd"         : to_float(row.get("flete_usd", "")),
            "seguro_usd"        : to_float(row.get("seguro_usd", "")),
            "cif_usd"           : to_float(row.get("valor_cif_usd", "")),
            "tasa_advalorem_pct": to_float(row.get("tasa_advalorem_pct", "")),
            "derechos_advalorem": to_float(row.get("derechos_advalorem", "")),
            "iva"               : to_float(row.get("iva", "")),
            "total_tributos"    : to_float(row.get("total_tributos", "")),
            "moneda"            : row.get("moneda", "").strip(),
        },

        # ── Metadata ────────────────────────────────
        "metadata": {
            "num_items" : to_int(row.get("num_items", "")),
            "creado_en" : datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    }
    return doc

# ──────────────────────────────────────────────
# Carga a MongoDB
# ──────────────────────────────────────────────

# Parámetros de conexión
MONGO_URI        = "mongodb://localhost:27017/"
MONGO_DATABASE   = "datasur"
MONGO_COLLECTION = "rutas"
BATCH_SIZE       = 200   # documentos por lote (upsert)

def load_to_mongodb(documents: list[dict],
                    uri: str        = MONGO_URI,
                    database: str   = MONGO_DATABASE,
                    collection: str = MONGO_COLLECTION,
                    batch_size: int = BATCH_SIZE) -> dict:
    """
    Carga una lista de documentos en MongoDB usando upsert por lotes.

    - Upsert por _id: si el documento ya existe lo actualiza,
      si no existe lo inserta. Esto hace la carga idempotente
      (se puede ejecutar múltiples veces sin duplicar datos).
    - Los documentos se envían en lotes (batch_size) para evitar
      saturar la conexión con colecciones muy grandes.

    Parámetros
    ----------
    documents   : lista de documentos transformados
    uri         : URI de conexión MongoDB  (default: localhost:27017)
    database    : nombre de la base de datos (default: datasur)
    collection  : nombre de la colección    (default: rutas)
    batch_size  : tamaño del lote           (default: 200)

    Retorna
    -------
    dict con { insertados, actualizados, errores, duracion_seg }
    """

    if not documents:
        print("⚠️  No hay documentos para cargar.")
        return {"insertados": 0, "actualizados": 0, "errores": 0, "duracion_seg": 0}

    print(f"\n── Conectando a MongoDB ─────────────────────────────────────────────")
    print(f"   URI        : {uri}")
    print(f"   Base datos : {database}")
    print(f"   Colección  : {collection}")
    print(f"   Documentos : {len(documents)}")
    print(f"   Lote size  : {batch_size}")
    print(f"────────────────────────────────────────────────────────────────────")

    t_inicio = datetime.now()

    # ── Verificar conexión ──────────────────────────────────────────────
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("✅  Conexión exitosa\n")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"❌  No se pudo conectar a MongoDB: {e}")
        print(f"    Verifica que el servidor esté activo en: {uri}")
        return {"insertados": 0, "actualizados": 0, "errores": 1, "duracion_seg": 0}

    col = client[database][collection]

    total_insertados  = 0
    total_actualizados = 0
    total_errores     = 0

    # ── Procesar en lotes ───────────────────────────────────────────────
    total_lotes = (len(documents) + batch_size - 1) // batch_size

    for n_lote, inicio in enumerate(range(0, len(documents), batch_size), start=1):
        lote = documents[inicio: inicio + batch_size]

        # Construir operaciones upsert
        operaciones = [
            UpdateOne(
                filter={"_id": doc["_id"]},
                update={"$set": doc},
                upsert=True
            )
            for doc in lote
        ]

        try:
            resultado = col.bulk_write(operaciones, ordered=False)
            insertados  = resultado.upserted_count
            actualizados = resultado.modified_count
            total_insertados   += insertados
            total_actualizados += actualizados

            print(f"   Lote {n_lote:>3}/{total_lotes}  "
                  f"│ +{insertados:>4} insertados  "
                  f"│ ~{actualizados:>4} actualizados")

        except BulkWriteError as bwe:
            errores_lote = len(bwe.details.get("writeErrors", []))
            total_errores += errores_lote
            print(f"   Lote {n_lote:>3}/{total_lotes}  ⚠️  {errores_lote} errores en este lote")
            for err in bwe.details.get("writeErrors", [])[:3]:
                print(f"      → doc _id={err.get('keyValue', {}).get('_id', '?')} : {err.get('errmsg', '')}")

    client.close()

    duracion = round((datetime.now() - t_inicio).total_seconds(), 2)

    # ── Reporte final ───────────────────────────────────────────────────
    print(f"""
── Resultado de carga ───────────────────────────────────────────────────────
   Insertados   : {total_insertados}
   Actualizados : {total_actualizados}
   Errores      : {total_errores}
   Duración     : {duracion}s
   Colección    : {uri}{database}.{collection}
────────────────────────────────────────────────────────────────────────────
""")

    return {
        "insertados"   : total_insertados,
        "actualizados" : total_actualizados,
        "errores"      : total_errores,
        "duracion_seg" : duracion,
    }

def load_json_to_mongodb(filepath: str, mongo_uri: str = "mongodb://localhost:27017/", 
                         db_name: str = "datasur", collection_name: str = "rutas") -> dict:
    """Carga el archivo JSON (array de documentos) a MongoDB local.

    Para cada documento hace un `replace_one` usando `_id` como clave, con `upsert=True`
    para evitar errores por duplicados.
    Retorna un dict con conteos: inserted/upserted/updated/errors.
    """
    result = {"processed": 0, "inserted": 0, "updated": 0, "errors": 0}

    if MongoClient is None:
        print("❌  pymongo no está instalado. Instala con: pip install pymongo / uv add pymongo")
        result["errors"] = 1
        return result

    if not os.path.exists(filepath):
        print(f"❌  Archivo JSON no encontrado: {filepath}")
        result["errors"] = 1
        return result

    with open(filepath, "r", encoding="utf-8") as f:
        try:
            docs = json.load(f)
        except Exception as e:
            print(f"❌  Error leyendo JSON: {e}")
            result["errors"] = 1
            return result

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        coll = db[collection_name]

        for doc in docs:
            result["processed"] += 1
            try:
                key = {"_id": doc.get("_id")} if doc.get("_id") is not None else None
                if key:
                    res = coll.replace_one(key, doc, upsert=True)
                    if getattr(res, "upserted_id", None) is not None:
                        result["inserted"] += 1
                    elif getattr(res, "matched_count", 0) > 0:
                        result["updated"] += 1
                    else:
                        result["inserted"] += 1
                else:
                    # Sin _id → insertar como nuevo documento
                    coll.insert_one(doc)
                    result["inserted"] += 1
            except Exception:
                result["errors"] += 1

        client.close()
    except Exception as e:
        print(f"❌  Error conectando a MongoDB: {e}")
        result["errors"] += 1

    return result


# ──────────────────────────────────────────────
# Ejecución
# ──────────────────────────────────────────────

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌  Archivo no encontrado: {INPUT_FILE}")
        sys.exit(1)

    documents = []
    errores   = []

    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            try:
                documents.append(transform_row(row))
            except Exception as e:
                errores.append({"fila": idx, "error": str(e)})

    # Escribir JSON array
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)

    # Intentar cargar el JSON recién generado en MongoDB local
    try:
        load_result = load_json_to_mongodb(OUTPUT_FILE)
        if load_result.get("errors", 0) == 0:
            print(f"✅  Carga a MongoDB completada: procesados={load_result.get('processed')}, insertados={load_result.get('inserted')}, actualizados={load_result.get('updated')}")
        else:
            print(f"⚠️  Carga a MongoDB con errores: {load_result}")
    except Exception as e:
        print(f"⚠️  Error al ejecutar la carga a MongoDB: {e}")

    # Reporte
    print(f"\n✅  Transformación completada")
    print(f"    Registros procesados : {len(documents)}")
    print(f"    Errores              : {len(errores)}")
    print(f"    Archivo generado     : {OUTPUT_FILE}")
    if errores:
        print(f"\n⚠️  Detalle de errores:")
        for e in errores[:10]:
            print(f"    Fila {e['fila']}: {e['error']}")
    print(f"\nSiguiente paso: importar el archivo JSON a MongoDB {load_result}")



if __name__ == "__main__":
    main()
