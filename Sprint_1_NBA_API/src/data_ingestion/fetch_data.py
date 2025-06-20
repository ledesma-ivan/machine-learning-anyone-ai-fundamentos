# src/data_ingestion/fetch_data.py

import os
import time
import pandas as pd
from nba_api.stats.endpoints import (
    commonallplayers,
    commonplayerinfo,
    playercareerstats,
    playerprofilev2,
)
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))


from src.utils.logger import setup_logger

logger = setup_logger(__name__)
RATE_LIMIT_SECONDS = 1.2
MAX_RETRIES = 3

def fetch_active_players(save_path: str = None) -> pd.DataFrame:
    """
    Obtiene la lista de jugadores activos usando commonallplayers.
    Guarda el DataFrame en save_path si se provee.
    """
    logger.info("Obteniendo lista de jugadores activos")
    df = commonallplayers.CommonAllPlayers(is_only_current_season=1).get_data_frames()[0]
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_csv(save_path, index=False)
        logger.info(f"Guardado active_players en {save_path}")
    return df

def _call_with_retries(fn, *args, **kwargs):
    """
    Llama a la función fn(*args, **kwargs) con reintentos y backoff progresivo.
    Retorna el resultado o None si falla tras MAX_RETRIES.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Espera antes de la llamada (si no es el primer intento, backoff mayor)
            time.sleep(RATE_LIMIT_SECONDS * attempt)
            return fn(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Intento {attempt}/{MAX_RETRIES} de {fn.__name__} falló: {e}")
            if attempt == MAX_RETRIES:
                logger.error(f"{fn.__name__} falló tras {MAX_RETRIES} intentos para args={args}, kwargs={kwargs}")
                return None
            # else: continuará al siguiente intento con mayor sleep

def fetch_player_info(player_id: int, save_path: str = None) -> pd.DataFrame:
    """
    Obtiene información general de un jugador. Reintenta hasta MAX_RETRIES.
    """
    def _inner(pid):
        return commonplayerinfo.CommonPlayerInfo(player_id=pid).get_data_frames()[0]
    df = _call_with_retries(_inner, player_id)
    if df is not None and save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_csv(save_path, index=False)
        logger.info(f"Guardado info de player {player_id} en {save_path}")
    return df

def fetch_player_career_stats(player_id: int, save_path: str = None) -> pd.DataFrame:
    """
    Obtiene estadísticas de carrera por temporada del jugador, con reintentos.
    """
    def _inner(pid):
        return playercareerstats.PlayerCareerStats(player_id=pid).get_data_frames()[0]
    df = _call_with_retries(_inner, player_id)
    if df is not None and save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_csv(save_path, index=False)
        logger.info(f"Guardado career stats de player {player_id} en {save_path}")
    return df

def fetch_player_profile(player_id: int, save_path: str = None) -> pd.DataFrame:
    """
    Obtiene splits y estadísticas avanzadas del jugador, con reintentos.
    """
    def _inner(pid):
        return playerprofilev2.PlayerProfileV2(player_id=pid).get_data_frames()[0]
    df = _call_with_retries(_inner, player_id)
    if df is not None and save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_csv(save_path, index=False)
        logger.info(f"Guardado profile de player {player_id} en {save_path}")
    return df

def fetch_all_players_info(players_df: pd.DataFrame, out_dir: str):
    """
    Para cada jugador en players_df, obtiene info, career stats y profile.
    Guarda cada uno en carpetas separadas y registra errores.
    """
    # Definir subdirectorios
    dir_info = os.path.join(out_dir, "info")
    dir_career = os.path.join(out_dir, "career")
    dir_profile = os.path.join(out_dir, "profile")
    os.makedirs(dir_info, exist_ok=True)
    os.makedirs(dir_career, exist_ok=True)
    os.makedirs(dir_profile, exist_ok=True)

    failed_ids = []
    for idx, row in players_df.iterrows():
        pid = row.get("PLAYER_ID")
        if pid is None:
            continue
        logger.info(f"Procesando player_id={pid} ({idx+1}/{len(players_df)})")
        # Rutas de guardado
        path_info = os.path.join(dir_info, f"{pid}.csv")
        path_career = os.path.join(dir_career, f"{pid}.csv")
        path_profile = os.path.join(dir_profile, f"{pid}.csv")

        df_info = fetch_player_info(pid, save_path=path_info)
        df_career = fetch_player_career_stats(pid, save_path=path_career)
        df_profile = fetch_player_profile(pid, save_path=path_profile)

        # Si alguna llamada falló, registramos y opcionalmente removemos archivos parciales
        if df_info is None or df_career is None or df_profile is None:
            logger.error(f"Fallo al obtener datos completos de player {pid}. Se omite este jugador.")
            failed_ids.append(pid)
            # Opcional: borrar archivos parciales si existen
            for p in [path_info, path_career, path_profile]:
                if os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            continue

    if failed_ids:
        logger.warning(f"Total jugadores con fallo: {len(failed_ids)}. IDs: {failed_ids}")

    # Opcional: concatenar todos los CSV en tres archivos globales
    def _concat_dir(dir_path, output_file):
        files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith(".csv")]
        if not files:
            logger.warning(f"No hay archivos CSV en {dir_path} para concatenar.")
            return None
        df_list = []
        for fp in files:
            try:
                df_list.append(pd.read_csv(fp))
            except Exception as e:
                logger.warning(f"No se pudo leer {fp}: {e}")
        if df_list:
            df_all = pd.concat(df_list, ignore_index=True)
            df_all.to_csv(output_file, index=False)
            logger.info(f"Concatenado {len(df_list)} archivos de {dir_path} en {output_file}")
            return df_all
        return None

    # Rutas de salida global
    all_info_path = os.path.join(out_dir, "all_players_info.csv")
    all_career_path = os.path.join(out_dir, "all_players_career.csv")
    all_profile_path = os.path.join(out_dir, "all_players_profile.csv")

    _concat_dir(dir_info, all_info_path)
    _concat_dir(dir_career, all_career_path)
    _concat_dir(dir_profile, all_profile_path)

    logger.info("fetch_all_players_info completado.")
