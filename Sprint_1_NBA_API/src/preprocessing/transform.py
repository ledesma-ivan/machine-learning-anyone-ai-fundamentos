import pandas as pd
from sklearn.preprocessing import StandardScaler

def load_all_data(base_path: str):
    """
    Carga los tres datasets combinados de info, career y profile.
    """
    info = pd.read_csv(f"{base_path}/all_players_info.csv")
    career = pd.read_csv(f"{base_path}/all_players_career.csv")
    profile = pd.read_csv(f"{base_path}/all_players_profile.csv")
    return info, career, profile

def clean_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y selección de columnas útiles de info general.
    """
    df_clean = df[[
        'PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'HEIGHT', 'WEIGHT', 
        'COUNTRY', 'POSITION', 'TEAM_NAME'
    ]].drop_duplicates()

    # Convertir altura tipo "6-7" a centímetros
    def height_to_cm(h):
        if isinstance(h, str) and '-' in h:
            ft, inches = map(int, h.split('-'))
            return round((ft * 12 + inches) * 2.54)
        return None

    df_clean['HEIGHT_CM'] = df_clean['HEIGHT'].apply(height_to_cm)
    df_clean['WEIGHT_KG'] = df_clean['WEIGHT'] * 0.453592

    df_clean = df_clean.drop(columns=['HEIGHT', 'WEIGHT'])
    return df_clean

def aggregate_career_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resume estadísticas de carrera por jugador (última temporada, promedio, etc.)
    """
    # Por simplicidad: usar promedios por jugador
    grouped = df.groupby("PLAYER_ID").mean(numeric_only=True).reset_index()
    return grouped

def merge_datasets(info_df, career_df, profile_df):
    """
    Une los datasets en un solo DataFrame.
    """
    career_df = aggregate_career_stats(career_df)
    merged = info_df.merge(career_df, left_on="PERSON_ID", right_on="PLAYER_ID", how="inner")
    # profile_df también podría agregarse si es útil
    return merged

def transform_pipeline(base_path: str, output_path: str, return_df: bool = False):
    """
    Ejecuta toda la transformación y guarda un CSV listo para modelado.
    Si return_df es True, también retorna el DataFrame procesado.
    """
    info, career, profile = load_all_data(base_path)
    info_clean = clean_info(info)
    merged = merge_datasets(info_clean, career, profile)
    merged.to_csv(output_path, index=False)
    print(f"Transformación completada: {output_path}")

    if return_df:
        return merged