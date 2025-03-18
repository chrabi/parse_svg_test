#!/usr/bin/env python3.12
"""
Skrypt do wykonywania zapytań GraphQL i zapisywania wyników do CSV.
Zapytanie dotyczy serwerów z tabeli HardwareInfo z filtrami na ID (1-4) i AppId (173456).
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests
from requests.exceptions import RequestException

# Konfiguracja loggera
def setup_logger() -> logging.Logger:
    """Konfiguracja systemu logowania."""
    logger = logging.getLogger("graphql_export")
    logger.setLevel(logging.INFO)
    
    # Handler dla logów w konsoli
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # Handler dla logów w pliku
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(f"{log_dir}/graphql_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Funkcja do wykonywania zapytania GraphQL
def execute_graphql_query(url: str, query: str, variables: Dict[str, Any], logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Wykonuje zapytanie GraphQL i zwraca wynik.
    
    Args:
        url: URL endpointu GraphQL
        query: Treść zapytania GraphQL
        variables: Zmienne dla zapytania
        logger: Obiekt loggera
        
    Returns:
        Słownik z odpowiedzią lub None w przypadku błędu
    """
    headers = {
        "Content-Type": "application/json",
        # Dodaj tutaj dodatkowe nagłówki, jeśli są wymagane (np. autoryzacja)
        # "Authorization": "Bearer YOUR_TOKEN"
    }
    
    payload = {
        "query": query,
        "variables": variables
    }
    
    try:
        logger.info(f"Wykonywanie zapytania GraphQL z zmiennymi: {variables}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if "errors" in data:
            logger.error(f"Błąd GraphQL: {data['errors']}")
            return None
            
        return data
        
    except RequestException as e:
        logger.error(f"Błąd podczas wykonywania zapytania: {str(e)}")
        return None
    except ValueError as e:
        logger.error(f"Błąd podczas parsowania odpowiedzi JSON: {str(e)}")
        return None

# Funkcja do przetwarzania danych i zapisywania do CSV
def process_and_save_to_csv(data: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Przetwarza dane z odpowiedzi GraphQL i zapisuje do pliku CSV.
    
    Args:
        data: Słownik z danymi odpowiedzi
        logger: Obiekt loggera
        
    Returns:
        True jeśli operacja się powiodła, False w przeciwnym wypadku
    """
    try:
        # Sprawdzenie, czy dane zawierają oczekiwaną strukturę
        if not data or "data" not in data:
            logger.error("Brak danych do przetworzenia")
            return False
            
        # Dostosuj tę ścieżkę do struktury Twoich danych GraphQL
        server_data = data.get("data", {}).get("hardwareInfo", [])
        
        if not server_data:
            logger.warning("Brak rekordów do zapisania")
            return False
            
        # Przygotowanie katalogu wyjściowego
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"output/{timestamp}/Inventory"
        os.makedirs(output_dir, exist_ok=True)
        
        # Nazwa pliku CSV
        current_date = datetime.now().strftime("%Y%m%d")
        output_file = f"{output_dir}/Inventory_{current_date}.csv"
        
        # Nagłówki CSV
        fieldnames = ["Id", "ServerName", "Serial", "AppId", "AppName", "TimestampEpoch"]
        
        # Przygotowanie danych do zapisania
        csv_data = []
        
        for server in server_data:
            # Dostosuj mapowanie pól do struktury Twoich danych
            server_row = {
                "Id": server.get("id"),
                "ServerName": server.get("serverName"),
                "Serial": server.get("serial"),
                "AppId": 173456,  # Zgodnie z zapytaniem
                "AppName": server.get("hardwareAppInfo", {}).get("appName"),
                "TimestampEpoch": int(time.time())
            }
            csv_data.append(server_row)
        
        # Zapisanie do CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
            
        logger.info(f"Zapisano {len(csv_data)} rekordów do pliku {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Błąd podczas przetwarzania danych: {str(e)}")
        return False

def main():
    """Główna funkcja programu."""
    logger = setup_logger()
    logger.info("Rozpoczęcie wykonywania skryptu")
    
    # URL endpointu GraphQL - zamień na właściwy adres
    graphql_url = "https://example.com/graphql"
    
    # Zapytanie GraphQL
    query = """
    query GetHardwareInfo($filter: HardwareInfoFilter, $first: Int!) {
      hardwareInfo(filter: $filter, first: $first) {
        id
        serverName
        serial
        hardwareAppInfo(filter: {appId: 173456}) {
          appId
          appName
        }
      }
    }
    """
    
    # Zmienne dla zapytania
    variables = {
        "filter": {
            "id": {"between": [1, 4]}
        },
        "first": 100
    }
    
    try:
        # Wykonanie zapytania
        result = execute_graphql_query(graphql_url, query, variables, logger)
        
        if not result:
            logger.error("Nie udało się wykonać zapytania GraphQL")
            sys.exit(1)
            
        # Przetworzenie i zapis do CSV
        if not process_and_save_to_csv(result, logger):
            logger.error("Nie udało się zapisać danych do pliku CSV")
            sys.exit(1)
            
        logger.info("Skrypt zakończył działanie pomyślnie")
        
    except Exception as e:
        logger.critical(f"Wystąpił nieoczekiwany błąd: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
