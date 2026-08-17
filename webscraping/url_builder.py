from datetime import datetime, timedelta
from typing import Callable, List, Dict 
import inspect

class urls_builder:
    def __init__(self, origin: str, destity: str, outbound_date: str):
        self.origin = origin 
        self.destity = destity
        self.outbound_date = outbound_date

    # LATAM
    @staticmethod
    def build_latam_url(origin, destination, outbound_date, adults=1, children=0, infants=0):
        """
        Generates a valid url for latam website (One Way).

        - origin: IATA code (ex: 'THE')
        - destination: IATA code (ex: 'GRU')
        - outbound_date: outbound_date format YYYY-MM-DD
        - adults: adults number
        - children: children number
        - infants: babies number
        """
        base_url = "https://www.latamairlines.com/br/pt/oferta-voos"
        
        url = (
            f"{base_url}?"
            f"origin={origin}"
            f"&outbound={outbound_date}T15%3A00%3A00.000Z"
            f"&destination={destination}"
            f"&adt={adults}"
            f"&chd={children}"
            f"&inf={infants}"
            f"&trip=OW"
            f"&cabin=Economy"
            f"&redemption=false"
            f"&sort=RECOMMENDED"
        )
        return url

        
    @staticmethod
    def gerar_urls(build_url_func: Callable[..., str], data: Dict) -> List[str]:
        origem = data.get("flight_from")
        destino = data.get("flight_to")
        start_date_str = data.get("start_date")
        final_date_str = data.get("final_date")

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        final_date = datetime.strptime(final_date_str, "%Y-%m-%d")

        delta = timedelta(days=1)
        current_date = start_date

        urls = []

        # Find out dynamically the parameter of the function
        func_params = inspect.signature(build_url_func).parameters
        possible_date_params = {"departure_date", "outbound_date"}
        date_param_name = next((p for p in func_params if p in possible_date_params), None)

        if not date_param_name:
            raise ValueError("Url function does not match with the mapped parameters.")

        while current_date <= final_date:
            formatted_date = current_date.strftime("%Y-%m-%d")

            url = build_url_func(
                origin=origem,
                destination=destino,
                **{date_param_name: formatted_date}
            )

            # Retornando apenas a string da URL diretamente, sem solicitation_id
            urls.append(url)

            current_date += delta

        return urls
        
    @staticmethod
    def build_skiplagged_url(origin: str, destination: str, departure_date: str) -> str:
        """
        origin: Origin IATA code (ex: 'THE')
        - destination: Destity IATA code (ex: 'GRU')
        - departure_date: outbound data on format 'YYYY-MM-DD'
        """
        base_url = "https://skiplagged.com/flights"
        return f"{base_url}/{origin.upper()}/{destination.upper()}/{departure_date}"