"""
This is the main module for the bigdata package.
It serves as the entry point for the package and can be used to execute any necessary setup or initialization code.
"""
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import json
import os

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# conectar a mongodb local
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

# Parámetros de conexión
MONGO_URI        = "mongodb://localhost:27017/"
MONGO_DATABASE   = "datasur"
MONGO_COLLECTION = "rutas"
BATCH_SIZE       = 200   # documentos por lote (upsert)

# crear funcion para verificar conexion a mongodb
def check_mongodb_connection(MONGO_URI):
    """Verifica la conexión a MongoDB."""
    if MongoClient is None:
        raise ImportError("pymongo no está instalado. Instala pymongo para usar esta función.")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # Verifica la conexión
        return True
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"Error de conexión a MongoDB: {e}")
        return False
    
# crea una funcion obtener una lista de registros de la base de datos, maximo 200 registros
def get_rutas():
    """Obtiene una lista de registros de la base de datos, máximo 200 registros."""
    if MongoClient is None:
        raise ImportError("pymongo no está instalado. Instala pymongo para usar esta función.")
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION] 
        rutas = list(collection.find().limit(BATCH_SIZE))
        return rutas
    except Exception as e:
        print(f"Error al obtener rutas: {e}")
        return []


def get_data(limit: int = 100, file_path: str = os.path.join("data", "registros_aduana_mongodb.json")):
    """Lee el archivo JSON local y devuelve hasta `limit` registros con las columnas especificadas.

    Las columnas devueltas son las claves literales:
    - 'importador.nombre'
    - 'manifiesto.numero'
    - 'exportador.pais'
    - 'embarque.naviera'
    - 'embarque.fecha_embarque'
    - 'aduana.codigo_pais'
    - 'aduana.nombre_aduana'

    Retorna una lista de diccionarios (posible longitud menor si hay menos registros).
    """
    def _get_nested(obj, *keys):
        cur = obj
        for k in keys:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
            if cur is None:
                return None
        return cur

    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception as e:
        print(f"Error al leer {file_path}: {e}")
        return []

    # Determinar la lista de registros en el JSON
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        # buscar la primera propiedad que sea una lista (si el JSON está envuelto)
        items = None
        for v in raw.values():
            if isinstance(v, list):
                items = v
                break
        if items is None:
            # si no hay lista, intentar usar el dict como único registro
            items = [raw]
    else:
        return []

    result = []
    for rec in items[:limit]:
        mapped = {
            "importador.nombre": _get_nested(rec, "importador", "nombre"),
            "manifiesto.numero": _get_nested(rec, "manifiesto", "numero"),
            "exportador.pais": _get_nested(rec, "exportador", "pais"),
            "embarque.naviera": _get_nested(rec, "embarque", "naviera"),
            "embarque.fecha_embarque": _get_nested(rec, "embarque", "fecha_embarque"),
            "aduana.codigo_pais": _get_nested(rec, "aduana", "codigo_pais"),
            "aduana.nombre_aduana": _get_nested(rec, "aduana", "nombre_aduana"),
        }
        result.append(mapped)

    return result
    

#rutas
@app.get("/",include_in_schema=False, name="home")
def home(request: Request):
    """Endpoint de prueba para verificar que la aplicación está funcionando correctamente."""
    return templates.TemplateResponse(request,"index.html")

# API endpoint para obtener una lista de registros de la base de datos
@app.get("/api/posts", response_class=JSONResponse)
def status():
    """Endpoint para obtener una lista de registros de la base de datos."""
    if not check_mongodb_connection(MONGO_URI):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo conectar a MongoDB")
    
    rutas = get_rutas()
    return JSONResponse(content={"rutas": rutas}, status_code=200)


@app.get("/rutas", response_class=HTMLResponse, name="rutas")
def rutas(request: Request):
    """Endpoint para obtener una lista de registros de rutas."""
    if not check_mongodb_connection(MONGO_URI):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo conectar a MongoDB")
    
    rutas = get_data()
    return templates.TemplateResponse("rutas.html", {"request": request, "rutas": rutas})