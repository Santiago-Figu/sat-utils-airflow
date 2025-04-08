
from datetime import datetime
from typing import Union, List, Optional

from sat_utils.satcfdi.utils import iterate

def construir_parametros_sat(
    RfcSolicitante: str, 
    FechaInicial: Optional[str] = None,
    FechaFinal: Optional[str] = None,
    RfcEmisor: Optional[str] = None,
    RfcReceptor: Union[str, List[str], None] = None,
    TipoSolicitud: Optional[str] = None,
    TipoComprobante: Optional[str] = None,
    EstadoComprobante: Optional[str] = None,
    RfcACuentaTerceros: Optional[str] = None,
    Complemento: Optional[str] = None,
    UUID: Optional[str] = None,
    RfcFIEL: Optional[str] = None  # RFC de la FIEL por si no se proporciona Emisor
) -> dict:
    """
    Construye un diccionario con los parámetros para consulta al SAT, con validaciones.

    Args:
        RfcSolicitante: RFC contenido en la FIEL
        FechaInicial: date en formato AAAA-MM-DD
        FechaFinal: date en formato AAAA-MM-DD
        RfcReceptor: string o list de los RFC Emisores (máx 5)
        RfcEmisor: string del RFC receptor (si no se proporciona, se usa RfcFIEL)
        TipoSolicitud: 'CFDI' o 'Metadata'
        TipoComprobante: None (Todos), 'I' (Ingreso), 'E' (Egreso), 'T' (Traslado), 'N' (Nomina), 'P' (Pago)
        EstadoComprobante: None (Todos), '0' (Cancelado), '1' (Vigente)
        RfcACuentaTerceros: RFC a cuenta de terceros
        Complemento: Complemento de CFDI a descargar
        UUID: Folio Fiscal para consulta específica
        RfcFIEL: RFC contenido en la FIEL (para usar como RfcReceptor por defecto)

    Returns:
        dict: Diccionario con los parámetros validados para la consulta al SAT

    Examples:
        Ejemplo 1 Consulta básica por fechas: 
            params1 = construir_parametros_sat(
                    FechaInicial="2023-01-01",
                    FechaFinal="2023-12-31",
                    RfcEmisor="XAXX010101000",
                    RfcReceptor="XAXX010101000",
                    TipoSolicitud="CFDI"
                )

        Ejemplo 2 Consulta con múltiples RFC emisores:
            params2 = construir_parametros_sat(
                FechaInicial="2023-01-01",
                FechaFinal="2023-01-31",
                RfcReceptor=["EKU9003173C9", "AAA010101AAA"],
                RfcFIEL="XAXX010101000",
                TipoComprobante="I",
                EstadoComprobante="1"
            )

        Ejemplo 3 Consulta por UUID:
            params3 = construir_parametros_sat(
                UUID="a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0",
                RfcFIEL="XAXX010101000"
            )
    """

    parametros = {
        "FechaInicial": None,
        "FechaFinal": None,
        "RfcEmisor": None,
        "RfcReceptores": None,
        "RfcSolicitante": RfcSolicitante,
        "TipoSolicitud": "CFDI",
        "TipoComprobante": None,
        "EstadoComprobante": None,
        "RfcACuentaTerceros": None,
        "Complemento": None,
        "Folio": None
    }

    # Validación de fechas
    if FechaInicial is not None or FechaFinal is not None:
        if FechaInicial is None or FechaFinal is None:
            raise ValueError("Debe proporcionar ambas fechas (inicial y final) o ninguna")

        try:
            fecha_inicial = datetime.strptime(FechaInicial, "%Y-%m-%d").date()
            fecha_final = datetime.strptime(FechaFinal, "%Y-%m-%d").date()
            if fecha_final < fecha_inicial:
                raise ValueError("FechaFinal no puede ser anterior a FechaInicial")
        except ValueError as e:
            raise ValueError(f"Formato de fecha inválido: {e}")

        parametros["FechaInicial"] = FechaInicial
        parametros["FechaFinal"] = FechaFinal

    # # Validación RFC Receptor
    if RfcReceptor is not None:
        if isinstance(RfcReceptor, str):
            rfc_emisores = [RfcReceptor]
        elif isinstance(RfcReceptor, list):
            if len(RfcReceptor) > 5:
                raise ValueError("No se permiten más de 5 RFC emisores")
            rfc_emisores = RfcReceptor
        else:
            raise ValueError("RfcReceptor debe ser string o lista de strings")

        # Validar formato básico de RFC (12-13 caracteres alfanuméricos)
        for rfc in rfc_emisores:
            if not (12 <= len(rfc) <= 13) or not rfc.isalnum():
                raise ValueError(f"RFC emisor inválido: {rfc}")

        parametros["RfcReceptores"] = [('RfcReceptor', r) for r in iterate(rfc_emisores)]

    # Validación RFC emisor
    if RfcEmisor is None and RfcFIEL is not None:
        RfcEmisor = RfcFIEL

    if RfcEmisor is not None:
        if not (12 <= len(RfcEmisor) <= 13) or not RfcEmisor.isalnum():
            raise ValueError(f"RFC emisor inválido: {RfcEmisor}")
        parametros["RfcEmisor"] = RfcEmisor

    # Validación TipoSolicitud
    if TipoSolicitud is not None:
        if TipoSolicitud.upper() not in ["CFDI", "METADATA"]:
            raise ValueError("TipoSolicitud debe ser 'CFDI' o 'Metadata'")
        parametros["TipoSolicitud"] = TipoSolicitud.capitalize() if TipoSolicitud.upper() == "METADATA" else TipoSolicitud.upper()

    # Validación TipoComprobante
    if TipoComprobante is not None:
        tipos_validos = [None, 'I', 'E', 'T', 'N', 'P']
        if TipoComprobante.upper() not in [t.upper() for t in tipos_validos if t is not None]:
            raise ValueError("TipoComprobante debe ser None, 'I', 'E', 'T', 'N' o 'P'")
        parametros["TipoComprobante"] = TipoComprobante.upper()
    else:
        None

    # Validación EstadoComprobante
    if EstadoComprobante is not None:
        if EstadoComprobante not in ['0', '1']:
            raise ValueError("EstadoComprobante debe ser None, '0' (Cancelado) o '1' (Vigente)")
        parametros["EstadoComprobante"] = EstadoComprobante

    # Validación RFC a cuenta de terceros
    if RfcACuentaTerceros is not None:
        if not (12 <= len(RfcACuentaTerceros) <= 13) or not RfcACuentaTerceros.isalnum():
            raise ValueError(f"RFC a cuenta de terceros inválido: {RfcACuentaTerceros}")
        parametros["RfcACuentaTerceros"] = RfcACuentaTerceros

    # Validación Complemento (solo verifica que sea string si se proporciona)
    if Complemento is not None:
        if not isinstance(Complemento, str):
            raise ValueError("Complemento debe ser string")
        parametros["Complemento"] = Complemento

    # Validación UUID
    if UUID is not None:
        uuid_str = str(UUID).replace("-", "").upper()
        if len(uuid_str) != 32 or not uuid_str.isalnum():
            raise ValueError("UUID inválido, debe ser un folio fiscal válido")
        parametros["Folio"] = UUID

    return parametros