
from datetime import datetime, timezone
import base64
import os
from pathlib import Path


def save_to_file(xml_content, filename="soap_request.xml",carpeta="request"):
    """Guarda el XML en la carpeta temp/tu_carpeta del proyecto"""
    # Crear la estructura de carpetas si no existe
    base_url= os.path.join(os.getcwd(),"temp")
    temp_dir = Path(base_url)
    temp_dir.mkdir(exist_ok=True)
    downloads_dir = Path(base_url,carpeta)
    downloads_dir.mkdir(exist_ok=True)
    
    # Ruta completa del archivo
    file_path = downloads_dir / filename
    
    # Guardar el contenido en el archivo
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(xml_content)
    
    print(f"Archivo guardado en: {file_path.absolute()}")
    return str(file_path.absolute())

def save_zip_from_base64(base64_content, filename="archivo.zip", carpeta="zips"):
    """
    Guarda datos decodificados desde Base64 en un archivo ZIP dentro de la carpeta temp/carpeta.
    
    Args:
        base64_content (str): Datos codificados en Base64.
        filename (str): Nombre del archivo ZIP (debe terminar en .zip).
        carpeta (str): Subcarpeta dentro de /temp/ donde se guardará el archivo.
    
    Returns:
        str: Ruta absoluta del archivo guardado.
    """
    # Crear la estructura de carpetas
    base_dir = Path(os.getcwd()) / "temp"
    base_dir.mkdir(exist_ok=True)  # Crear /temp/ si no existe
    target_dir = base_dir / carpeta
    target_dir.mkdir(exist_ok=True)  # Crear /temp/carpeta/ si no existe
    
    # Decodificar el Base64 a bytes
    binary_data = base64.b64decode(base64_content)
    
    # Ruta completa del archivo ZIP
    file_path = target_dir / filename
    
    # Guardar los bytes en el archivo (modo binario 'wb')
    with open(file_path, "wb") as f:
        f.write(binary_data)
    
    print(f"Archivo ZIP guardado en: {file_path.absolute()}")
    return str(file_path.absolute())



def check_Expires(fecha_expira: str) -> bool:
    """
    Verifica si una fecha de expiración (en formato 'YYYY-MM-DD HH:MM:SS.ffffff') ya pasó.
    Usa UTC como referencia y elimina la zona horaria para comparar correctamente.

    Args:
        fecha_expira (str): Fecha en formato '2025-04-09 23:42:40.864000'.
    
    Returns:
        bool: True si la fecha ya caducó, False si aún es válida.
    """
    try:
        # Convertir la fecha de expiración a un objeto datetime (sin zona horaria)
        fecha_expira_dt = datetime.strptime(fecha_expira, "%Y-%m-%d %H:%M:%S.%f")
        
        # Obtener la fecha/hora actual en UTC y remover la zona horaria
        ahora_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        
        # Comparar (ambas fechas ahora son "naive")
        return ahora_utc > fecha_expira_dt
    except ValueError as e:
        raise ValueError(f"Formato de fecha inválido: {fecha_expira}. Se esperaba 'YYYY-MM-DD HH:MM:SS.ffffff'") from e