import os
from pathlib import Path
import sys
import requests
from lxml import etree
# Agregar la ruta del proyecto a sys.path para ejecución en local

sys.path.append(str(Path(__file__).parent.parent))

from sat_utils.models.evento_solicitud_descarga import EventosSolicitudDescarga
from sat_utils.utils.request_utils import construir_parametros_sat
from sat_utils.utils.utils import save_to_file
from sat_utils.satcfdi.pacs.sat import _CFDIAutenticacion, _CFDISolicitaDescarga, _CFDIVerificaSolicitudDescarga
from sat_utils.satcfdi.models.signer import Signer
from sat_utils.satcfdi.utils import iterate, parser
from sat_utils.satcfdi.exceptions import ResponseError
from config.log.logger import Logger

# from bs4 import BeautifulSoup

logger = Logger(file_name="sat_downloader", debug= True).get_logger()

class SATDownloader:
    def __init__(self, password_file:str, cert_file:str, key_file:str,  rfc:str = "EWE1709045U0"):
        self.rfc = rfc
        # sustituir posteriormente por descarga de archivos desde bucket o lectura desde Base de datos
        self.path_certificate = os.path.join(os.getcwd(),'testing','certificados',rfc, cert_file)
        self.path_key = os.path.join(os.getcwd(),'testing','certificados',rfc, key_file)
        self.path_password = os.path.join(os.getcwd(),'testing','certificados',rfc, password_file)
        self.signer = None
        self.wait_time = 60 # tiempo de espera recomendado para verificar estatus de la solicitud de descarga
        
    def _create_signer(self):
        logger.info("Cargando FIEL...")
        try:
            self.signer = Signer.load(
                certificate=open(self.path_certificate, 'rb').read(),
                key=open(self.path_key, 'rb').read(),
                password=open(self.path_password, 'r').read()
            )
            logger.info(f"✓ FIEL cargada correctamente (RFC del Certificado: {str(self.signer.rfc)[:4]})")
        except Exception as e:
            mensaje = f"Ocurrio un error al cargar la FIEL: {e}"
            logger.error(mensaje)
            raise Exception(mensaje)

    def _get_headers(self,soap_action, needs_token_fn) -> dict:
        headers = {
            'Content-type': 'text/xml;charset="utf-8"',
            'Accept': 'text/xml',
            'Cache-Control': 'no-cache',
            # "User-Agent": __version__.__user_agent__,
            'SOAPAction': soap_action
        }
        if needs_token_fn:
            headers['Authorization'] = f'WRAP access_token="{needs_token_fn}"'
        logger.debug(f"headers: {headers}")
        return headers
    
    def send_soap_request(self,soap_url, data, soap_action, needs_token_fn, verify=True):
        logger.debug(f"Generando petición a: {soap_url}")
        response = requests.post(
            url=soap_url,
            data=data,
            headers=self._get_headers(soap_action, needs_token_fn=needs_token_fn),
            verify=verify
        )
        if not response.ok:
            mensaje = f"No fue posible realizar la petición SOAP, status_code:{response.status_code} - response:{response.content}"
            logger.error(mensaje)
            raise ResponseError(mensaje)

        return etree.fromstring(
            response.content,
            parser=parser
        )

    def _login_sat(self):

        logger.info("Autenticando con SAT...")
         # Crear la instancia de autenticación
        auth_request = _CFDIAutenticacion(
            signer=self.signer,
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

        logger.info("Enviando petición SOAP:")
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
            logger.info(f"Token: {result['AutenticaResult'][:30]}")
            logger.info(f"Válido desde: {result['Created']}")
            logger.info(f"Válido hasta: {result['Expires']}")
            return result
        except Exception as e:
            mensaje = f"No fue posible procesar la respuesta, error:{e}, data_response:{response}"
            logger.error(mensaje)
            raise Exception(mensaje)

    def _solicitar_descarga(self, token_auth:str):

        """
        Arguments:
            token_auth: token de autorización generado por el SAT
            FechaInicial: date en formato AAAA-MM-DD,
            FechaFinal: date en formato AAAA-MM-DD,
            RfcEmisor: string o list de los RFC Emisores, solo permite un máximo de 5,
            RfcReceptor: string del RFC receptor, si no se ingresa valor debería tomarce el valor del RFC contenido en la FIEL,
            TipoSolicitud: Tipo de solicitud que se quiere realizar al SAT, puede ser de CFDI o Metadata. Nota: se debe respetar el formato, CFDI mayúsculas| Metadata Capítal
            TipoComprobante: Valor opcional, Null es el valor predeterminado y en caso de no declararse se obtendrán todos los comprobantes sin importar el tipo comprobante. En caso de definirse los valores posibles son (Null = Todos, I = Ingreso, E = Egreso, T= Traslado, N = Nomina, P = Pago).
            EstadoComprobante: Valor opcional, Null es el valor predeterminado y en caso de no declararse se obtendrán todos los comprobantes sin importar su estado. En caso de definirse los valores posibles son (Null= Todos, 0 = Cancelado, 1 = Vigente), Nota: actualmente los CFDI cancelados solo pueden ser consultados por metadata.
            RfcACuentaTerceros: Valor opcional, Contiene el RFC del a cuenta a tercero del cual se quiere consultar los CFDIs.
            Complemento: Valor opcional, Define el complemento de CFDI a descargar,
            UUID: Valor opcional, Folio Fiscal en caso de querer realizar consulta especifica de un CFDI.
        Returns:
            dict: respuesta de solicitud de descarga
        """

        logger.info("Cargando FIEL...")
        id_solicitud = None
        try:
            logger.info(f" Preparando argumentos de consulta")

            # ToDo: Considerar recibir un diccionario de parametros hacer el get y contruir los parametros para la petición
            #  o recibirlos desde la función

            argumments = construir_parametros_sat(
                RfcSolicitante = str(self.signer.rfc),
                FechaInicial="2023-03-01",
                FechaFinal="2023-03-30",
                RfcReceptor=str(self.signer.rfc),
                TipoSolicitud="CFDI",
                RfcFIEL= None
            )

            logger.debug(f" argumments:{argumments}")

            logger.info("Generando auth con SAT...")
            # Crear la instancia para descarga
            download_request = _CFDISolicitaDescarga(
                signer=self.signer,
                arguments=argumments
            )

            logger.info("Enviando petición SOAP descarga:")

            # Configurar parámetros para send_soap_request
            soap_url = download_request.soap_url
            soap_action = download_request.soap_action
            needs_token_fn = token_auth  # Token obtenido del login
            verify = True  # Cambiar a False solo para desarrollo/testing

            # Obtener el payload SOAP
            soap_envelope = download_request.get_payload()
            logger.info("SOAP Envelope Donwload generado:")
            save_to_file(xml_content=soap_envelope.decode('utf-8'), filename="soap_request_download.xml")

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
                result = download_request.process_response(response)
                if int(result['CodEstatus']) == 5000:
                    logger.info("\n✅ Solicitud de descarga realizada correctamente!")
                    logger.info(result)
                    try:
                        id_solicitud = result['IdSolicitud']
                    except Exception as e:
                        logger.error("No fu posible obtener el ide de solicitud directamente")
                        id_solicitud = result
                else:
                    # mensaje = EventosSolicitudDescarga.obtener_mensaje(300)
                    logger.error(f"No fue posible generar la petición de descarga:{result['Mensaje']} - CodEstatus:{result['CodEstatus']}")
            except Exception as e:
                mensaje = f"No fue posible procesar la respuesta, error:{e}, data_response:{response}"
                logger.error(mensaje)
                raise Exception(mensaje)
        except Exception as e:
            mensaje = f"Ocurrio un error al cargar la FIEL: {e}"
            logger.error(mensaje)
            raise Exception(mensaje)
        finally:
            return id_solicitud
        
    def _recover_comprobante_status(self, token_auth:str, id_solicitud: str = "6e7b8a81-8da1-4116-8d40-157d7f9f67ad"):
        logger.info(f" Preparando argumentos para consulta de status de solicitud")
        arguments={
            'RfcSolicitante': self.signer.rfc,
            'IdSolicitud': id_solicitud,
        }
        # Crear la instancia para descarga
        status_request = _CFDIVerificaSolicitudDescarga(
            signer=self.signer,
            arguments=arguments
        )

        # Obtener el payload SOAP
        soap_envelope = status_request.get_payload()
        logger.info("SOAP Envelope check status generado:")
        save_to_file(xml_content=soap_envelope.decode('utf-8'), filename="soap_request_status.xml")

        logger.info("Enviando petición SOAP check status de petición")
        # Configurar parámetros para send_soap_request
        soap_url = status_request.soap_url
        soap_action = status_request.soap_action
        needs_token_fn = token_auth  # Token obtenido del login
        verify = True  # Cambiar a False solo para desarrollo/testing

        # Enviar la solicitud SOAP
        response = self.send_soap_request(
            soap_url=soap_url,
            data=soap_envelope,
            soap_action=soap_action,
            needs_token_fn=needs_token_fn,
            verify=verify
        )
        result = status_request.process_response(response)
        #  response: {'IdsPaquetes': ['6E7B8A81-8DA1-4116-8D40-157D7F9F67AD_01'], 
        # 'CodEstatus': '5000', 'EstadoSolicitud': 3, 'CodigoEstadoSolicitud': '5000', 'NumeroCFDIs': 7, 'Mensaje': 'Solicitud Aceptada'}
        logger.info(f"response: {result}")
        if int(result['CodEstatus']) == 5000:
            logger.info("\n✅ Solicitud de status de descarga realizada correctamente!")

            logger.debug(f"VerificaSolicitudDescargaResult: {result['EstadoSolicitud']}")
        else:
            logger.error(f"No fue posible realizar la solicitud de descarga")
        
    
    
if __name__ == "__main__":
    # rfc = "EWE1709045U0"
    rfc = "FIGS901021ID8"
    # rfc = "EKU9003173C9"
    try:
        sat_downloader = SATDownloader(
            rfc = rfc,
            cert_file = f'{rfc}.cer',
            key_file = f'{rfc}.key',
            password_file = f'password.txt',
        )

        sat_downloader._create_signer()
        
        data_login_sat = sat_downloader._login_sat()
        
        #almacenar id de solicitud para realizar la descarga posteriormente
        # id_solicitud = sat_downloader._solicitar_descarga(token_auth=data_login_sat['AutenticaResult'])

        id_solicitud = "6e7b8a81-8da1-4116-8d40-157d7f9f67ad"
        
        # id_solicitud = None
        if id_solicitud:
            logger.debug(f"id_solicitud:{id_solicitud}")
            sat_downloader._recover_comprobante_status(token_auth=data_login_sat['AutenticaResult'],id_solicitud=id_solicitud)
        else:
            logger.error(f"No fue posible generar el id de solicitud: {id_solicitud}")
    except Exception:
        pass