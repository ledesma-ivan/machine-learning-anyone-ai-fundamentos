Este proyecto proporciona una API para acceder a datos de la NBA. Incluye puntos finales para recuperar información de jugadores y equipos de la NBA, estadísticas de partidos y otros datos relacionados con la NBA.
---

## Configuración del entorno

### 1. Crear un entorno virtual

```bash
python3 -m venv venv
```

### 2. Activar el entorno virtual

```bash
source venv/bin/activate
```

- Si estás en **Windows**, la activación del entorno virtual es:

```bash
.\venv\Scripts\activate
```

> **¿Para qué sirve esto?**  
Nos permite mantener el proyecto en onda, para que todos podamos correr el mismo codigo, sin diferencias de las versiones ya que eso nos puede generar conflictos en las depedencias.
---

## Instalacion de dependencias

Una vez activado el entorno virtual, ejecutá:

```bash
pip install -r requirements.txt
```