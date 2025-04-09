class EventosSolicitudDescarga:
    """
    Clase para manejar los códigos de evento y mensajes relacionados con solicitudes de descarga.
    """
    
    _eventos = {
        300: {
            'mensaje': 'Usuario No Válido',
            'observaciones': ''
        },
        301: {
            'mensaje': 'XML Mal Formado',
            'observaciones': 'Este código de error se regresa cuando el request posee información inválida, ejemplo: un RFC de receptor no válido.'
        },
        302: {
            'mensaje': 'Sello Mal Formado',
            'observaciones': ''
        },
        303: {
            'mensaje': 'Sello no corresponde con RfcSolicitante',
            'observaciones': ''
        },
        304: {
            'mensaje': 'Certificado Revocado o Caduco',
            'observaciones': 'El certificado fue revocado, o bien, la fecha de vigencia expiró.'
        },
        305: {
            'mensaje': 'Certificado Inválido',
            'observaciones': 'El certificado puede ser inválido por múltiples razones como son el tipo, codificación incorrecta, entre otros.'
        },
        5000: {
            'mensaje': 'Solicitud recibida con éxito',
            'observaciones': ''
        },
        5003: {
            'mensaje': 'Tope máximo de elementos de la consulta',
            'observaciones': 'La solicitud sobrepasa el máximo de resultados por tipo de solicitud (Metadata y CFDI)'
        },
        5004: {
            'mensaje': 'No se encontró la información',
            'observaciones': 'No se encontró la información de la solicitud de descarga que se pretende verificar.'
        },
        5011: {
            'mensaje': 'Límite de descargas por folio por día',
            'observaciones': 'Se ha alcanzado o sobrepasado el límite de descargas diarias por folio.'
        }
    }
    
    @classmethod
    def obtener_mensaje(cls, codigo):
        """
        Obtiene el mensaje correspondiente a un código de evento.
        
        Args:
            codigo (int): El código del evento a consultar.
            
        Returns:
            str: El mensaje asociado al código, o None si el código no existe.
        """
        evento = cls._eventos.get(codigo)
        return evento['mensaje'] if evento else None
    
    @classmethod
    def obtener_observaciones(cls, codigo):
        """
        Obtiene las observaciones correspondientes a un código de evento.
        
        Args:
            codigo (int): El código del evento a consultar.
            
        Returns:
            str: Las observaciones asociadas al código, o None si el código no existe.
        """
        evento = cls._eventos.get(codigo)
        return evento['observaciones'] if evento else None
    
    @classmethod
    def obtener_info_completa(cls, codigo):
        """
        Obtiene toda la información disponible para un código de evento.
        
        Args:
            codigo (int): El código del evento a consultar.
            
        Returns:
            dict: Un diccionario con 'mensaje' y 'observaciones', o None si el código no existe.
        """
        return cls._eventos.get(codigo)