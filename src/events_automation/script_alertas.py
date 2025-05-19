import logging
from logging.handlers import TimedRotatingFileHandler
import requests
from datetime import datetime, timedelta, timezone
import json

class BatchProcessor:
     """
    Clase para gestionar la obtención y procesamiento de batches fallidos, 
    además de notificar los resultados a Google Chat.
    """
    def __init__(self, base_url, dataset_id, headers, webhook_url, log_file="script_log.log", days_back=1):
        """
        Inicializa la clase con los parámetros básicos.
        
        :param base_url: URL base de la API para obtener los datos.
        :param dataset_id: Identificador del dataset que se está procesando.
        :param headers: Encabezados para las solicitudes HTTP.
        :param webhook_url: URL del webhook de Google Chat para enviar notificaciones.
        """
        self.base_url = base_url
        self.dataset_id = dataset_id
        self.headers = headers
        self.webhook_url = webhook_url
        self.days_back = days_back

        # Configuración de logging
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                TimedRotatingFileHandler(
                    log_file,
                    when="W0",  # Rotar semanalmente, cada lunes
                    interval=1,
                    backupCount=4
                ),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_failed_batches_data(self):
        """Obtiene y procesa los batches fallidos."""
        self.logger.info(f"Iniciando obtención de batches fallidos para dataset_id: {self.dataset_id}")
        now = datetime.now(timezone.utc)
        start_date = now - timedelta(days=self.days_back)
        params = {
            "dataSet": self.dataset_id,
            "createdAfter": int(start_date.timestamp() * 1000),
            "createdBefore": int(now.timestamp() * 1000),
            "status": "failed",
            "orderBy": "asc:created"
        }

        try:
            self.logger.debug(f"Realizando llamada a {self.base_url} con params: {params}")
            response = requests.get(self.base_url, params=params, headers=self.headers)
            response.raise_for_status()
            failed_batches = response.json()
            self.logger.info(f"Se obtuvieron {len(failed_batches)} batches fallidos.")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error al obtener batches fallidos: {e}")
            return None, None, f"<b>Error HTTP:</b>\n\n {str(e)}"

        if not failed_batches:
            self.logger.info("No se encontraron batches fallidos.")
            return None, None, None

        return self._process_failed_batches(failed_batches)

    def _process_failed_batches(self, failed_batches):
        failed_urls = {}
        for batch_id, batch_info in failed_batches.items():
            failed_batch_location = batch_info.get("failedBatchLocation")
            if failed_batch_location:
                try:
                    self.logger.debug(f"Procesando ubicación de batch fallido: {failed_batch_location}")
                    response = requests.get(failed_batch_location, headers=self.headers)
                    response.raise_for_status()
                    data = response.json()
                    failed_urls[batch_id] = [
                        item.get("_links", {}).get("self", {}).get("href")
                        for item in data.get("data", [])
                        if item.get("_links", {}).get("self", {}).get("href")
                    ]
                except requests.RequestException as e:
                    self.logger.error(f"Error al procesar {failed_batch_location}: {e}")

        batch_data_map = {}
        for batch_id, urls in failed_urls.items():
            batch_data_map[batch_id] = []
            for url in urls:
                try:
                    self.logger.debug(f"Solicitando datos de evento desde {url}")
                    response = requests.get(url, headers=self.headers)
                    response.raise_for_status()
                    json_objects = response.text.splitlines()
                    for json_obj in json_objects:
                        try:
                            data = json.loads(json_obj)
                            xdm_entity = data.get("body", {}).get("xdmEntity", {})
                            event_type = xdm_entity.get("eventType", "No disponible")
                            webpage_url = xdm_entity.get("web", {}).get("webPageDetails", {}).get("URL", "No disponible")
                            batch_data_map[batch_id].append({
                                "eventType": event_type,
                                "webPageURL": webpage_url
                            })
                        except json.JSONDecodeError:
                            self.logger.warning(f"Error de decodificación JSON en la URL {url}")
                            batch_data_map[batch_id].append({
                                "eventType": "Error de unión",
                                "webPageURL": "No disponible"
                            })
                except requests.RequestException as e:
                    self.logger.error(f"Error al procesar la URL {url}: {e}")
                    batch_data_map[batch_id].append({
                        "eventType": "Error",
                        "webPageURL": "Error"
                    })

        self.logger.info("Procesamiento de batches fallidos completado.")
        return failed_batches, batch_data_map, None

    def build_card_message(failed_batches, processed_batches, days_back, dataset_id, error_message=None):
        """Construye el mensaje en formato JSON para Google Chat."""

        if error_message:
            return {
                "cards": [
                    {
                        "header": {
                            "title": "<b> ¡Algo ha salido mal! </b>",
                            "subtitle": f"Enviado: <b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>",
                            "imageUrl": "https://png.pngtree.com/png-vector/20230904/ourmid/pngtree-fail-stamp-grungy-png-image_9932568.png"
                        },
                        "sections": [
                            {
                                "widgets": [
                                    {
                                        "textParagraph": {
                                            "text": error_message
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }

        if not failed_batches:
            return {
                "cards": [
                    {
                        "header": {
                            "title": "<b> Informe de alertas </b>",
                            "subtitle": f"Enviado: <b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>",
                            "imageUrl": "https://cdn-icons-png.freepik.com/256/16206/16206597.png?semt=ais_hybrid"
                        },
                        "sections": [
                            {
                                "widgets": [
                                    {
                                        "textParagraph": {
                                            "text": f"No se encontraron errores en las últimas <b>{int(days_back * 24)}</b> horas.\n\n"
                                            f"<b>Dataset ID:</b> {dataset_id}"
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }

        card_message = {
            "cards": [
                {
                    "header": {
                        "title": "<b> Informe de alertas </b>",
                        "subtitle": f"Enviado: <b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>",
                        "imageUrl": "https://cdn-icons-png.flaticon.com/512/559/559384.png"
                    },
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "textParagraph": {
                                        "text": f"Se han consultado las últimas <b>{int(days_back * 24)}</b> horas. \nNúmero total de errores: <b>{len(failed_batches)}</b>"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        for batch_id, batch_info in failed_batches.items():
            related_objects = batch_info.get("relatedObjects", [])
            errors = batch_info.get("errors", [])
            tags = batch_info.get("tags", [])

            dataset_id = related_objects[0].get("id") if related_objects else "No disponible"
            error_code = errors[0].get("code") if errors else "No disponible"
            error_description = errors[0].get("description") if errors else "No disponible"
            flow_id = tags.get("flowId", ["No disponible"])[0]

            batch_details = (
                f"<b>Batch ID:</b> {batch_id}<br>\n"
                f"<b>&nbsp;&nbsp;• Dataflow:</b> {flow_id}<br>"
                f"<b>&nbsp;&nbsp;• Dataset ID:</b> {dataset_id}<br>"
                f"<b>&nbsp;&nbsp;• Código de error:</b> {error_code}<br>"
                f"<b>&nbsp;&nbsp;• Descripción:</b> {error_description}<br>"
            )

            section = {
                "widgets": [
                    {
                        "textParagraph": {
                            "text": batch_details
                        }
                    }
                ]
            }

            if batch_id in processed_batches:
                for record in processed_batches[batch_id]:
                    event_details = (
                        f"<b>&nbsp;&nbsp;• Tipo de evento:</b> {record['eventType']}\n\n<br>"
                    )
                    section["widgets"].append({
                        "textParagraph": {
                            "text": event_details
                        }
                    })
                    section["widgets"].append({
                        "keyValue": {
                            "topLabel": "<b>Página de origen del evento</b>",
                            "content": record['webPageURL'],
                            "contentMultiline": True
                        }
                    })
            else:
                section["widgets"].append({
                    "textParagraph": {
                        "text": "No se encontraron eventos para este batch."
                    }
                })

            card_message["cards"][0]["sections"].append(section)

        return card_message

    def post_message_to_google_chat(self, card_message):
        """Envía el mensaje a Google Chat utilizando un webhook."""
        if card_message is None or not card_message.get("cards"):
            self.logger.error("El mensaje está vacío y no se enviará.")
            return

        response = requests.post(self.webhook_url, json=card_message, headers={"Content-Type": "application/json"})
        if response.status_code != 200:
            self.logger.error(f"Error al enviar el mensaje. Código de respuesta: {response.status_code}")
            self.logger.error(response.text)
        else:
            self.logger.info("Mensaje enviado exitosamente a Google Chat.")

    def process_and_notify(self):
        """Función principal para buscar batches fallidos y enviar el mensaje a Google Chat."""
        failed_batches, processed_batches, error_message = self.get_failed_batches_data()
        card_message = self.build_card_message(failed_batches, processed_batches, error_message)
        self.post_message_to_google_chat(card_message)



if __name__ == "__main__":
    processor = BatchProcessor(
        base_url="https://example.com/api",
        dataset_id="dataset123",
        headers={"Authorization": "Bearer token"},
        webhook_url="https://chat.googleapis.com/v1/spaces/...",
        days_back=1
    )
    processor.process_and_notify()


