import os
from pathlib import Path
import sys
import requests
from lxml import etree
# Agregar la ruta del proyecto a sys.path para ejecución en local

sys.path.append(str(Path(__file__).parent.parent))

from sat_utils.utils.utils import save_to_file
from sat_utils.satcfdi.pacs.sat import _CFDIAutenticacion
from sat_utils.satcfdi.models.signer import Signer
from sat_utils.satcfdi.utils import iterate, parser
from sat_utils.satcfdi.exceptions import ResponseError
from config.log.logger import Logger

# from bs4 import BeautifulSoup

logger = Logger(file_name="sat_downloader").get_logger()

class SATDownloader:
    def __init__(self, password_file:str, cert_file:str, key_file:str,  rfc:str = "EWE1709045U0"):
        self.rfc = rfc
        # sustituir posteriormente por descarga de archivos desde bucket o lectura desde Base de datos
        self.path_certificate = os.path.join(os.getcwd(),'testing','certificados',rfc, cert_file)
        self.path_key = os.path.join(os.getcwd(),'testing','certificados',rfc, key_file)
        self.path_password = os.path.join(os.getcwd(),'testing','certificados',rfc, password_file)

    def _get_headers(self,soap_action, needs_token_fn) -> dict:
        headers = {
            'Content-type': 'text/xml;charset="utf-8"',
            'Accept': 'text/xml',
            'Cache-Control': 'no-cache',
            # "User-Agent": __version__.__user_agent__,
            'SOAPAction': soap_action
        }
        if needs_token_fn:
            headers['Authorization'] = f'WRAP access_token="{needs_token_fn()}"'
        return headers
    
    def send_soap_request(self,soap_url, data, soap_action, needs_token_fn, verify=True):
        response = requests.post(
            url=soap_url,
            data=data,
            headers=self._get_headers(soap_action, needs_token_fn=needs_token_fn),
            verify=verify
        )


        if not response.ok:
            mensaje = f"No fue posible generar el token de autentificación, status_code:{response.status_code} - response:{response}"
            logger.error(mensaje)
            raise ResponseError(mensaje)

        return etree.fromstring(
            response.content,
            parser=parser
        )

    def _login_sat(self):

        logger.info("Cargando FIEL...")
        try:
            signer = Signer.load(
                certificate=open(self.path_certificate, 'rb').read(),
                key=open(self.path_key, 'rb').read(),
                password=open(self.path_password, 'r').read()
            )

            logger.info(f"✓ FIEL cargada correctamente (RFC del Certificado: {signer.rfc})")
        except Exception as e:
            mensaje = f"Ocurrio un error al cargar la FIEL: {e}"
            logger.error(mensaje)
            raise Exception(mensaje)

        logger.info("Autenticando con SAT...")
         # Crear la instancia de autenticación
        auth_request = _CFDIAutenticacion(
            signer=signer,
            arguments={
                "seconds": 300  # 5 minutos de validez
            }
        )

        # Obtener el payload SOAP
        soap_envelope = auth_request.get_payload()
        logger.info("SOAP Envelope generado:")
        save_to_file(soap_envelope.decode('utf-8'))

         # Configurar parámetros para send_soap_request
        soap_url = auth_request.soap_url
        soap_action = auth_request.soap_action
        needs_token_fn = None  # No necesitas token para autenticación inicial
        verify = True  # Cambiar a False solo para desarrollo/testing

        
        # Enviar la solicitud SOAP
        response = self.send_soap_request(
            soap_url=soap_url,
            data=soap_envelope,
            soap_action=soap_action,
            needs_token_fn=needs_token_fn,
            verify=verify
        )

        try:
            # Procesar la respuesta
            result = auth_request.process_response(response)
            logger.info("\n✅ Autenticación exitosa!")
            logger.info(f"Token: {result['AutenticaResult']}")
            logger.info(f"Válido desde: {result['Created']}")
            logger.info(f"Válido hasta: {result['Expires']}")
            return result
        except Exception as e:
            mensaje = f"No fue posible procesar la respuesta, error:{e}, data_response:{response}"
            logger.error(mensaje)
            raise Exception(mensaje)


    
    
if __name__ == "__main__":
    rfc = "EWE1709045U"
    # rfc = "FIGS901021ID8"
    try:
        sat_downloader = SATDownloader(
            rfc = rfc,
            cert_file = f'{rfc}.cer',
            key_file = f'{rfc}.key',
            password_file = f'password.txt',
        )

        data_login_sat = sat_downloader._login_sat()
    except Exception:
        pass