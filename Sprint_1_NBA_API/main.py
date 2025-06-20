import os
from src.data_ingestion.fetch_data import fetch_active_players, fetch_all_players_info
from src.utils.logger import setup_logger
from src.preprocessing.transform import transform_pipeline
import argparse



def main(max_players: int):
    logger = setup_logger("main")

    output_dir = "data/raw"
    os.makedirs(output_dir, exist_ok=True)

    # Paso 1: Obtener jugadores activos
    players_path = os.path.join(output_dir, "active_players.csv")
    players_df = fetch_active_players(save_path=players_path)

    logger.info(f"Columnas disponibles: {players_df.columns.tolist()}")

    # Paso 1.5: Limitar a los primeros N jugadores activos
    limited_df = players_df.head(max_players)
    logger.info(f"Se seleccionaron los primeros {max_players} jugadores activos: {limited_df['PERSON_ID'].tolist()}")

    # Paso 2: Obtener info detallada de los jugadores
    fetch_all_players_info(limited_df.rename(columns={"PERSON_ID": "PLAYER_ID"}), out_dir=output_dir)

    logger.info(f"Pipeline ETL completado con {max_players} jugadores.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline ETL para datos NBA")
    parser.add_argument(
        "--max_players",
        type=int,
        default=1,
        help="Cantidad máxima de jugadores a procesar (default=1)",
    )
    args = parser.parse_args()
    main(max_players=args.max_players)