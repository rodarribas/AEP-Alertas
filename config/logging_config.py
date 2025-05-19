import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os

class LoggerManager:
    def __init__(self, log_directory='logs', max_bytes=5 * 1024 * 1024, backup_count=3):
        """
        Inicializa el gestor de logging.
        
        :param log_directory: Directorio donde se guardarán los logs.
        :param max_bytes: Tamaño máximo en bytes de cada archivo de log.
        :param backup_count: Número máximo de backups por fecha.
        """
        self.log_directory = log_directory
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.logger = self._configure_logging()
        current_date = datetime.now().strftime("%d-%m-%Y")
        self._cleanup_old_logs(current_date)



    def _configure_logging(self):
        """
        Configurar Logger principal
        """
        # Directorio donde se guardarán los logs
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

        # Nombre del archivo de log
        current_date = datetime.now().strftime("%d-%m-%Y")
        log_filename = os.path.join(self.log_directory, f'app_{current_date}.log')

        # Configuración del logger
        logger = logging.getLogger()  
        logger.setLevel(logging.DEBUG)  # Nivel global de logging

        # Formato de los logs
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)

        # Configura el handler para la consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)

        # Configura el handler para el archivo de log con rotación
        file_handler = RotatingFileHandler(
            log_filename, maxBytes=self.max_bytes, backupCount=self.backup_count  # Tamaño máx: 5MB, 5 copias
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)

        # Agregar los handlers al logger
        if not logger.handlers:  # Evita agregar múltiples handlers en caso de múltiples llamadas
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger
    
    def _cleanup_old_logs(self, current_date):
        """
        Elimina archivos antiguos de logs si exceden el límite de backups por fecha.

        :param current_date: Fecha actual en formato dd-MM-yyyy.
        """
        log_files = [
            f for f in os.listdir(self.log_directory)
            if f.startswith(f'app_{current_date}') and f.endswith('.log')
        ]
        log_files.sort()  # Ordena los archivos para garantizar que los más antiguos sean los primeros

        # Si hay más de `backup_count` logs, elimina los más antiguos
        while len(log_files) > self.backup_count:
            file_to_remove = log_files.pop(0)
            os.remove(os.path.join(self.log_directory, file_to_remove))

    def get_logger(self):
        """
        Devuelve el logger configurado.
        """
        return self.logger