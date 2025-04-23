import os
from pathlib import Path
import sys
import time
import requests
from lxml import etree
# sys.path para ejecución en local
sys.path.append(str(Path(__file__).parent.parent))
from sat_utils.models.evento_solicitud_descarga import EventosSolicitudDescarga
from sat_utils.utils.request_utils import construir_parametros_sat
from sat_utils.utils.utils import check_Expires, save_to_file, save_zip_from_base64
from sat_utils.satcfdi.pacs.sat import _CFDIAutenticacion, _CFDIDescargaMasiva, _CFDISolicitaDescarga, _CFDIVerificaSolicitudDescarga, EstadoSolicitud
from sat_utils.satcfdi.models.signer import Signer
from sat_utils.satcfdi.utils import iterate, parser
from sat_utils.satcfdi.exceptions import ResponseError
from config.log.logger import Logger

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
        #Nota: repetir la carga en cada parte, ya que parece que en ejecución con Airflow, no se guarda el valor del signer en la clase por alguna razon
        logger.info("Cargando FIEL...")
        try:
            signer = Signer.load(
                certificate=open(self.path_certificate, 'rb').read(),
                key=open(self.path_key, 'rb').read(),
                password=open(self.path_password, 'r').read()
            )
            logger.info(f"✓ FIEL cargada correctamente (RFC del Certificado: {str(signer.rfc)[:4]})")
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

    def _solicitar_descarga(self, token_auth:str, FechaInicial: str ="2023-04-01", FechaFinal:str ="2023-04-30",TipoSolicitud: str ="CFDI"):

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
        try:
            signer = Signer.load(
                certificate=open(self.path_certificate, 'rb').read(),
                key=open(self.path_key, 'rb').read(),
                password=open(self.path_password, 'r').read()
            )
            logger.info(f"✓ FIEL cargada correctamente (RFC del Certificado: {str(signer.rfc)[:4]})")
        except Exception as e:
            mensaje = f"Ocurrio un error al cargar la FIEL: {e}"
            logger.error(mensaje)
            raise Exception(mensaje)
        
        id_solicitud = None
        try:
            logger.info(f" Preparando argumentos de consulta")

            # ToDo: Considerar recibir un diccionario de parametros hacer el get y contruir los parametros para la petición
            #  o recibirlos desde la función

            argumments = construir_parametros_sat(
                RfcSolicitante = str(signer.rfc),
                FechaInicial = FechaInicial,
                FechaFinal = FechaFinal,
                RfcReceptor = str(signer.rfc),
                TipoSolicitud = TipoSolicitud,
                RfcFIEL= None
            )

            logger.debug(f" argumments:{argumments}")

            logger.info("Generando auth con SAT...")
            # Crear la instancia para descarga
            download_request = _CFDISolicitaDescarga(
                signer=signer,
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
        logger.info(f"Preparando argumentos para consulta de status de solicitud")
        try: 
            logger.info("Cargando FIEL...")
            try:
                signer = Signer.load(
                    certificate=open(self.path_certificate, 'rb').read(),
                    key=open(self.path_key, 'rb').read(),
                    password=open(self.path_password, 'r').read()
                )
                logger.info(f"✓ FIEL cargada correctamente (RFC del Certificado: {str(signer.rfc)[:4]})")
            except Exception as e:
                mensaje = f"Ocurrio un error al cargar la FIEL: {e}"
                logger.error(mensaje)
                raise Exception(mensaje)

            arguments={
                'RfcSolicitante': signer.rfc,
                'IdSolicitud': id_solicitud,
            }
            # Crear la instancia para descarga
            status_request = _CFDIVerificaSolicitudDescarga(
                signer=signer,
                arguments=arguments
            )

            # Obtener el payload SOAP
            soap_envelope = status_request.get_payload()
            logger.info(f"[{signer.rfc}] SOAP Envelope check status generado:")
            save_to_file(xml_content=soap_envelope.decode('utf-8'), filename="soap_request_status.xml")

            logger.info(f"[{signer.rfc}] Enviando petición SOAP check status de petición")
            # Configurar parámetros para send_soap_request
            soap_url = status_request.soap_url
            soap_action = status_request.soap_action
            needs_token_fn = token_auth  # Token obtenido del login
            verify = True  # Cambiar a False solo para desarrollo/testing

            result = None
            # Enviar la solicitud SOAP
            response = self.send_soap_request(
                soap_url=soap_url,
                data=soap_envelope,
                soap_action=soap_action,
                needs_token_fn=needs_token_fn,
                verify=verify
            )
            result = status_request.process_response(response)
            # ejemplo de respuesta del SAT
            #  response: {'IdsPaquetes': ['6E7B8A81-8DA1-4116-8D40-157D7F9F67AD_01'], 
            # 'CodEstatus': '5000', 'EstadoSolicitud': 3, 'CodigoEstadoSolicitud': '5000', 'NumeroCFDIs': 7, 'Mensaje': 'Solicitud Aceptada'}
            logger.debug(f"[{signer.rfc}] response: {result}")
            if int(result['CodEstatus']) == 5000:
                logger.info(f"\n[{signer.rfc}] ✅ Solicitud de status de descarga realizada correctamente!")
                logger.debug(f"[{signer.rfc}] VerificaSolicitudDescargaResult: {result}")
            else:
                logger.error(f"[{signer.rfc}] No fue posible realizar la solicitud de descarga")
        except Exception as e:
            logger.error(f'[{signer.rfc}] Ocurrio un error inesperado al realizar la petión SOAP Error: {e} - result value:{result}')
            result = None
        finally:
            return result
        

    def _recover_comprobante_download(self, id_paquete: str, token_auth: str):

        logger.info("Cargando FIEL...")
        try:
            signer = Signer.load(
                certificate=open(self.path_certificate, 'rb').read(),
                key=open(self.path_key, 'rb').read(),
                password=open(self.path_password, 'r').read()
            )
            logger.info(f"✓ FIEL cargada correctamente (RFC del Certificado: {str(signer.rfc)[:4]})")
        except Exception as e:
            mensaje = f"Ocurrio un error al cargar la FIEL: {e}"
            logger.error(mensaje)
            raise Exception(mensaje)

        logger.debug(f"[{signer.rfc}] Descargando paquete {id_paquete}")

        logger.info(f"[{signer.rfc}] Preparando argumentos para consulta de status de solicitud")
        
        download_request = _CFDIDescargaMasiva(
            signer=signer,
            arguments={
                'RfcSolicitante': signer.rfc,
                'IdPaquete': id_paquete,
            }
        )

        # Obtener el payload SOAP
        soap_envelope = download_request.get_payload()
        logger.info(f"[{signer.rfc}] SOAP Envelope check status generado:")
        save_to_file(xml_content=soap_envelope.decode('utf-8'), filename="soap_request_download.xml")


        logger.info(f"[{signer.rfc}] Enviando petición SOAP check status de petición")
        # Configurar parámetros para send_soap_request
        soap_url = download_request.soap_url
        soap_action = download_request.soap_action
        needs_token_fn = token_auth  # Token obtenido del login
        verify = True  # Cambiar a False solo para desarrollo/testing
        
        result = None
        try: 
            # Enviar la solicitud SOAP
            response = self.send_soap_request(
                soap_url=soap_url,
                data=soap_envelope,
                soap_action=soap_action,
                needs_token_fn=needs_token_fn,
                verify=verify
            )
            result, paquete = download_request.process_response(response)
            logger.debug(f"[{signer.rfc}] response: {result}")
            if int(result['CodEstatus']) == 5000:
                logger.info(f"\n[{signer.rfc}] ✅ Paquete descargado correctamente!")
                logger.debug(f"[{signer.rfc}] DescargaResult: {result}")
                # logger.debug(f"paquete {paquete}")
                #Guardar paquet en .zip
                save_zip_from_base64(base64_content=paquete, filename=f"{id_paquete}.zip", carpeta="cfdi")

            else:
                logger.error(f"[{signer.rfc}] No fue posible realizar la descarga del paquete")
        except Exception as e:
            logger.error(f'[{signer.rfc}] Ocurrio un error inesperado al realizar la petión SOAP Error: {e} - result value:{result}')
            result = None
        finally:
            return result
    
    
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
        
        data_login_sat = sat_downloader._login_sat()
        
        #almacenar id de solicitud para realizar la descarga posteriormente
        # id_solicitud = sat_downloader._solicitar_descarga(token_auth=data_login_sat['AutenticaResult'])

        # id_solicitud = "6e7b8a81-8da1-4116-8d40-157d7f9f67ad"
        id_solicitud = "303f2187-d4a5-49d9-a1be-b0c264d7deef"
        if id_solicitud:
            notFinished = True
            i = 3
            while notFinished:
                logger.debug(f"id_solicitud:{id_solicitud}")
                if check_Expires(str(data_login_sat['Expires'])):
                    logger.warning(f"[{sat_downloader.rfc}] Token expirado, renovando...")
                    data_login_sat = sat_downloader._login_sat()
                else:
                    logger.debug(f"[{sat_downloader.rfc}] El token sigue vigente")
                data_status = sat_downloader._recover_comprobante_status(token_auth=data_login_sat['AutenticaResult'],id_solicitud=id_solicitud)
                if data_status:
                    # ejemplo de respuesta del SAT
                    #  response: {'IdsPaquetes': ['6E7B8A81-8DA1-4116-8D40-157D7F9F67AD_01'], 
                    # 'CodEstatus': '5000', 'EstadoSolicitud': 3, 'CodigoEstadoSolicitud': '5000', 'NumeroCFDIs': 7, 'Mensaje': 'Solicitud Aceptada'}
                    if data_status['EstadoSolicitud'] == EstadoSolicitud.TERMINADA:
                        # código para solicitud terminada 3, esto indica que el paquete de descarga de comprobantes se realizo correctamente 
                        notFinished = False # bandera para salir del ciclo de solicitud de estatus:
                        #Flujo para realizar descarga de los paquetes de CFDI
                        logger.debug(f"[{sat_downloader.rfc}] Solicitud procesada correctamente, CFDIs a descargar {data_status['NumeroCFDIs']} en {len(data_status['IdsPaquetes'])} paquetes")
                        for paquete in data_status['IdsPaquetes']:
                            #ToDo: verificar caducidad del token, en caso de caducar, volver a generar
                            data_download= sat_downloader._recover_comprobante_download(id_paquete=paquete,token_auth=data_login_sat['AutenticaResult'])
                    elif data_status['EstadoSolicitud'] in [EstadoSolicitud.ACEPTADA, EstadoSolicitud.EN_PROCESO]:
                        # Generar el sleep para reintentar la nueva solicitud
                        # Todo, considerar una estrategia de reintentos para no saturar el sistema, podría ser un un incremento del Sleep pasados ciertos intentos
                        logger.debug(f"[{sat_downloader.rfc}] La solicitud aun no ha sido procesada por el SAT, reintentando solicitud en {sat_downloader.wait_time} segundos, estado de la solicitud {data_status['EstadoSolicitud']}")
                        time.sleep(sat_downloader.wait_time)
                        i+=1
                        continue
                    elif data_status['EstadoSolicitud'] == EstadoSolicitud.RECHAZADA:
                        logger.debug(f"[{sat_downloader.rfc}] La solicitud de descarga fue rechazada, verifique los datos")
                        break
                    elif data_status['EstadoSolicitud'] == EstadoSolicitud.VENCIDA:
                        logger.debug(f"[{sat_downloader.rfc}] La solicitud de descarga ha vencido, volver a generar la petición")
                        break
                    elif data_status['EstadoSolicitud'] == EstadoSolicitud.ERROR:
                        logger.debug(f"[{sat_downloader.rfc}] La solicitud de descarga ha generado un error, verificar los datos")
                        break
                    else:
                        logger.error(f"Estado de solicitud no identificado, verificar EstadoSolicitud:{data_status['EstadoSolicitud']}")
                break
                # end if
            # end while
        # end if
        else:
            logger.error(f"No fue posible generar el id de solicitud: {id_solicitud}")
    except Exception:
        pass