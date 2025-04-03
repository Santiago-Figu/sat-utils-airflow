

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
    
    print(f"XML guardado en: {file_path.absolute()}")
    return str(file_path.absolute())