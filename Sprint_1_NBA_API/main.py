# src/main.py

import os
from src.data_ingestion.fetch_data import fetch_active_players, fetch_all_players_info
from src.utils.logger import setup_logger

def main():
    logger = setup_logger("main")

    # Carpeta de salida
    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)

    # Paso 1: Obtener jugadores activos
    players_path = os.path.join(output_dir, "active_players.csv")
    players_df = fetch_active_players(save_path=players_path)

    # Mostrar columnas disponibles para debug
    logger.info(f"Columnas disponibles: {players_df.columns.tolist()}")

    # Filtrar los primeros 10 jugadores
    top_players_df = players_df.head(10)
    logger.info(f"Se seleccionaron los primeros 10 jugadores activos: {top_players_df['PERSON_ID'].tolist()}")

    # Paso 2: Obtener info detallada de los jugadores (renombrar columna a 'PLAYER_ID')
    fetch_all_players_info(top_players_df.rename(columns={"PERSON_ID": "PLAYER_ID"}), out_dir=output_dir)

    logger.info("Pipeline ETL completado con 10 jugadores.")

if __name__ == "__main__":
    main()
