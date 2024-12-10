import pandas as pd
from pyairtable import Api
import re
from datetime import datetime, time
import hashlib
import json

# Configurar Airtable
base_id = 'appAYimbUheznQQOk'
accions_table_name = 'Accions'
assistents_table_name = 'Persones'
programa_table_name = 'Programa'
tecnics_table_name = 'Tècnic'
api_key = 'patLkBfCxA1ZEmVEP.181f72ab43a5c29e98f0bda540ce7bfecac01860789b7b91f5bae1a1a9d7edcf'
api = Api(api_key)

# Inicializar tablas
accions_table = api.table(base_id, accions_table_name)
assistents_table = api.table(base_id, assistents_table_name)
programa_table = api.table(base_id, programa_table_name)
tecnics_table = api.table(base_id, tecnics_table_name)

# Función para crear un hash único para el registro, enfocándose en campos clave inmutables
def create_record_hash(record):
    # Función auxiliar para convertir a fecha si es posible
    def convert_to_date(date_value):
        if pd.notna(date_value):
            try:
                # Intentar convertir a datetime si es un string
                if isinstance(date_value, str):
                    return pd.to_datetime(date_value).strftime('%Y-%m-%d')
                # Si ya es un datetime, convertir a string
                elif hasattr(date_value, 'strftime'):
                    return date_value.strftime('%Y-%m-%d')
                else:
                    return None
            except:
                return None
        return None

    # Campos para generar el hash que sean menos propensos a cambios menores
    hash_data = {
        'Assistents': sorted(record.get('Assistents', [])),  # IDs de asistentes
        'Programa': sorted(record.get('Programa', [])),  # IDs de programa
        'Tècnic': sorted(record.get('Tècnic', [])),  # IDs de técnico
        'Nom': record.get('Nom', '').strip(),  # Nombre de la sesión (sin espacios extras)
        'Data inici (día)': convert_to_date(record.get('Data inici')),
        'Data de fi (día)': convert_to_date(record.get('Data de fi')),
    }
    
    # Convertir a JSON para asegurar consistencia, ignorando campos que pueden cambiar fácilmente
    hash_string = json.dumps(hash_data, sort_keys=True)
    
    # Generar hash MD5
    return hashlib.md5(hash_string.encode()).hexdigest()

# Función para obtener registros existentes en la tabla "Accions"
def get_existing_records():
    records = accions_table.all(fields=['Identificador_Únic', 'Assistents', 'Nom', 'Data inici', 'Data de fi'])
    existing = {}
    for record in records:
        fields = record['fields']
        # Usar el Identificador_Únic si existe, de lo contrario crear uno
        identifier = fields.get('Identificador_Únic')
        if identifier:
            existing[identifier] = record['id']
    return existing

# Función para obtener el ID de Airtable de un asistente dado su NIF
def get_assistent_id(NIF):
    records = assistents_table.all(fields=['NIF'])
    for record in records:
        if record['fields'].get('NIF') == NIF:
            return record['id']
    print(f"No se encontró ID para el NIF: {NIF}")
    return None

# Función para obtener el ID del programa dado su nombre
def get_programa_id(nombre_programa):
    records = programa_table.all(fields=['Nom'])
    for record in records:
        if record['fields'].get('Nom') == nombre_programa:
            return [record['id']]
    print(f"No se encontró ID para el programa: {nombre_programa}")
    return []

# Función para obtener el ID del técnico dado su nombre
def get_tecnic_id(nombre_tecnic):
    records = tecnics_table.all(fields=['Nom'])
    for record in records:
        if record['fields'].get('Nom') == nombre_tecnic:
            return [record['id']]
    print(f"No se encontró ID para el técnico: {nombre_tecnic}")
    return []

# Preprocesar la columna de Assistents
def preprocess_assistents(value):
    if isinstance(value, str):
        NIFs = re.findall(r'\b\d+[A-Z]\b', value)
        ids = [get_assistent_id(NIF) for NIF in NIFs]
        valid_ids = [id for id in ids if id is not None]
        print(f"NIFs encontrados: {NIFs}, IDs válidos: {valid_ids}")
        return valid_ids
    return []

# Leer el archivo Excel
df = pd.read_excel('../xlsx/Informe.xlsx')

# Función para combinar fechas y horas de manera robusta
def combine_datetime(date_col, time_col):
    def combine(row):
        try:
            # Convertir fecha a datetime
            date = pd.to_datetime(row[date_col])
            
            # Convertir hora a time
            if pd.notna(row[time_col]):
                # Intentar parsear la hora
                if isinstance(row[time_col], time):
                    hour_time = row[time_col]
                else:
                    hour_time = pd.to_datetime(str(row[time_col])).time()
                
                # Combinar fecha y hora
                return pd.Timestamp.combine(date.date(), hour_time)
            else:
                # Si no hay hora, usar medianoche
                return pd.Timestamp.combine(date.date(), time(0, 0))
        except Exception as e:
            print(f"Error combinando fecha y hora para {date_col}, {time_col}: {e}")
            return None
    
    return df.apply(combine, axis=1)

# Aplicar combinación de fechas
df['Data inici'] = combine_datetime('Data d\'inici', 'Hora d\'inici d\'atenció')
df['Data de fi'] = combine_datetime('Data de fi', 'Hora fi d\'atenció')

# Preprocesar las columnas
df['Assistents'] = df['Assistents'].apply(preprocess_assistents)
df['Programa'] = df['Programa'].apply(get_programa_id)
df['Tècnic'] = df['Tècnic'].apply(get_tecnic_id)

# Mapeo de columnas
column_mapping = {
    'Programa': 'Programa',
    'Tècnic': 'Tècnic',
    'Sessió': 'Nom',
    'Data inici': 'Data inici',
    'Data de fi': 'Data de fi',
    'Modalitat': 'Modalitat',
    'Assistents': 'Assistents'
}

# Seleccionar y renombrar las columnas necesarias
df_airtable = df[column_mapping.keys()].rename(columns=column_mapping)

# Convertir las columnas de fecha a formato string con el formato deseado
df_airtable['Data inici'] = pd.to_datetime(df_airtable['Data inici'])
df_airtable['Data de fi'] = pd.to_datetime(df_airtable['Data de fi'])
df_airtable['Data inici'] = df_airtable['Data inici'].dt.strftime('%Y-%m-%d %H:%M')
df_airtable['Data de fi'] = df_airtable['Data de fi'].dt.strftime('%Y-%m-%d %H:%M')

# Generar identificador único para cada registro
df_airtable['Identificador_Únic'] = df_airtable.apply(create_record_hash, axis=1)

# Función para verificar y preparar datos antes de enviarlos
def prepare_record_data(record_data):
    # Verificar que los campos sean listas de IDs
    for col in ['Assistents', 'Programa', 'Tècnic']:
        if col in record_data:
            if isinstance(record_data[col], tuple):
                record_data[col] = list(record_data[col])
            elif not isinstance(record_data[col], list):
                record_data[col] = [] if pd.isna(record_data[col]) else [record_data[col]]
    return record_data

# Justo antes de la función process_records, añade esto:
def debug_record_processing(df):
    print("Total de registros en el DataFrame:", len(df))
    for index, row in df.iterrows():
        print(f"\nRegistro {index + 1}:")
        print("Identificador Único:", row['Identificador_Únic'])
        print("Nom:", row['Nom'])
        print("Data inici:", row['Data inici'])
        print("Data de fi:", row['Data de fi'])
        print("Modalitat:", row['Modalitat'])
        print("Assistents:", row['Assistents'])
        print("Tècnic:", row['Tècnic'])
        print("Programa:", row['Programa'])

        
# Procesar registros
def process_records(df, existing_records):
    debug_record_processing(df)  # Añadir esta línea para depuración
    
    for index, row in df.iterrows():
        # Obtener el identificador único
        identifier = row['Identificador_Únic']

        # Preparar datos del registro
        record_data = row.to_dict()
        record_data = prepare_record_data(record_data)

        if identifier in existing_records:
            # Actualizar registro existente
            record_id = existing_records[identifier]
            print(f"Actualizando registro existente con ID: {record_id}")
            try:
                accions_table.update(record_id, record_data)
                print(f"Registro actualizado: {record_id}")
            except Exception as e:
                print(f"Error al actualizar registro {record_id}: {str(e)}")
        else:
            # Crear nuevo registro
            print("\nCreando nuevo registro...")
            print("Detalles del registro:", record_data)
            try:
                new_record = accions_table.create(record_data)
                print(f"Registro creado con éxito. ID: {new_record['id']}")
            except Exception as e:
                print(f"Error al crear nuevo registro: {str(e)}")
                # Imprimir detalles completos del error
                print(f"Detalles completos del error: {e}")
                # Si es un error de Airtable, imprimir la respuesta completa
                if hasattr(e, 'response'):
                    print("Respuesta de Airtable:", e.response.text)

# Obtener registros existentes en Airtable
existing_records = get_existing_records()

# Procesar registros
process_records(df_airtable, existing_records)


# In[ ]:




