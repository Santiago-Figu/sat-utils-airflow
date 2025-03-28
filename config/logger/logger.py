import logging
import os
import colorlog

class Logger:
    __logger = None

    def __init__(self, file_name: str = "looger"):
        # crear la carpeta logs si no existe
        log_dir = os.path.join(os.getcwd(),"logs","logger")
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)

        # Dar nombre al archivo de log
        log_file_name = file_name+".log"
        log_file_path = os.path.join(log_dir,log_file_name)

        #configuración del logger
        self.__logger = logging.getLogger(file_name)
        self.__logger.setLevel(logging.INFO)

        #Manejo de Handlers para evitar que se dupliquen instancias del logger si ya existen
        if not self.__logger.handlers:
            # Formato para archivo de salida
            file_formatter = self.__get_file_formatter__()

            #Formato para salida de consola
            console_formatter = self.__get_console_formatter__()

            # Handler para salida de archivo
            file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
            file_handler.setFormatter(file_formatter)
            self.__logger.addHandler(file_handler)

            # Handler para salida de consola
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(console_formatter)
            self.__logger.addHandler(console_handler)

    def get_logger(self):
        """Devuelve el logger configurado."""
        return self.__logger
    
    def __get_file_formatter__(self):
        '''Devuelve el formato de salida para el archivo'''
        file_formatter = logging.Formatter(
                '[%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d] %(message)s',
                datefmt='%d/%m/%Y %H:%M:%S'
            )
        return file_formatter
    
    def __get_console_formatter__(self):
        '''Devuelve el formato de salida para consola'''
        console_formatter = colorlog.ColoredFormatter(
                '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d] %(message)s',
                datefmt='%d/%m/%Y %H:%M:%S',
                log_colors={
                    'INFO': 'green',          
                    'WARNING': 'yellow',     
                    'ERROR': 'red',         
                    'CRITICAL': 'red,bg_white',  # Mensajes CRITICAL en rojo con fondo blanco
                }
            )
        return console_formatter