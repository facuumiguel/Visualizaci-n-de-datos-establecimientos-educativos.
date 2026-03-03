"""
INTEGRANTES:
    Echarri Abril
    Alice Maite
    Miguel Facundo

En primer lugar importamos las tablas que nos pasan por consigna,
luego calculamos las metricas, seguimos con la limpieza y la creacion de nuestras tablas,
continuamos con las consultas sql y finalmente con los graficos pedidos
"""

import pandas as pd
import duckdb as dd
import matplotlib.pyplot as plt
#%%
# Cargamos las tablas que nos pasan por consigna

Establecimientos_educativos = pd.read_excel('2022_padron_oficial_establecimientos_educativos.xlsx', skiprows=6)
Bibliotecas_populares = pd.read_csv('bibliotecas-populares.csv')
censo = pd.read_excel('padron_poblacion.xlsX', skiprows=12) #se saltean las primeras 12 filas pues no contienen información relevante
censo = censo.loc[:56583]

#%% 
#                               METRICAS
#%%
#PROBLEMA: CONSISTENCIA
# Seleccionar la columna de teléfono y convertirla a string (renombramos por claridad)
Establecimientos_educativos['Teléfono'] = Establecimientos_educativos['Teléfono'].astype(str).str.strip()

# Identificar registros inconsistentes
inconsistentes = Establecimientos_educativos[
    (Establecimientos_educativos['Teléfono'].str.len() != 9) |
    (Establecimientos_educativos['Teléfono'].str[4] != '-') |
    (Establecimientos_educativos['Teléfono'].str.contains(r'[^0-9-]', regex=True))
]

# Calcular porcentaje de inconsistencias
porcentaje_inconsistentes = 100 * len(inconsistentes) / len(Establecimientos_educativos)

# Mostrar resultado
print(f"Porcentaje de teléfonos inconsistentes: {porcentaje_inconsistentes:.2f}%")

#%%
#PROBLEMA: COMPLETITUD

# Reemplazar strings vacíos o con solo espacios por None (NaN)
Establecimientos_educativos['Común'] = Establecimientos_educativos['Común'].replace(r'^\s*$', None, regex=True)

# Total de filas
total = len(Establecimientos_educativos)

# Contar valores no nulos y no vacíos (completos)
completos = Establecimientos_educativos['Común'].notnull().sum()

# Calcular porcentaje de incompletitud (valores vacíos o nulos)
porcentaje_incompletos = 100 * (total - completos) / total

print(f"Porcentaje de valores incompletos en la columna 'Común': {porcentaje_incompletos:.2f}%")

#%%
#PROBLEMA: COMPLETITUD

def metrica_GQM_mail(Bibliotecas_populares):
    """
    Devuelve el porcentaje de valores vacíos o nulos en la columna 'mail'
    """
    nulls = Bibliotecas_populares['mail'].isna().sum()  # cantidad de nulos en 'mail'
    total = len(Bibliotecas_populares) # total de filas

    return (nulls * 100) / total

resultado = metrica_GQM_mail(Bibliotecas_populares)
print(f"Porcentaje de valores nulos o vacíos en la columna 'mail': {resultado:.2f}%")

#%%
#                               LIMPIEZA DE DATOS
#%%
# TABLA POBLACION

def separar_por_niveles(df_edades):
    """
    Agrupa a las edades segun el nivel educativo correspondiente.
    """
    
    #Arrancamos con un diccionario donde vamos acumulando la cantidad de personas por grupo etario
    grupo_etario = {
        'jardin': 0,
        'primaria': 0,
        'secundaria': 0,
        'total': 0
    }

    #Recorremos fila por fila, sumando cada edad en el grupo que le corresponde
    for _, row in df_edades.iterrows():
        cantidad = row['Cantidad']
        grupo = row['Edad']

        grupo_etario['total'] += cantidad
        
        # Clasificamos según edad
        if 0 <= grupo <= 5:
            grupo_etario['jardin'] += cantidad
        elif 6 <= grupo <= 12:
            grupo_etario['primaria'] += cantidad
        elif 13 <= grupo <= 18:
            grupo_etario['secundaria'] += cantidad
            
    # Armamos las nuevas filas para agregar al df
    nuevas_filas = [
        {'Grupo_Etario':'jardin', 'Cantidad' : grupo_etario['jardin']},
        {'Grupo_Etario':'primaria','Cantidad' : grupo_etario['primaria']},
        {'Grupo_Etario':'secundaria','Cantidad' : grupo_etario['secundaria']},
        {'Grupo_Etario':'total', 'Cantidad' : grupo_etario['total']}
    ]
    
    df_nivel_educativo = df_edades.rename(columns={'Edad': 'Grupo_Etario'})
    
    #Vaciamos el df y le agregamos los nuevos datos ya agrupados
    df_nivel_educativo = df_nivel_educativo.iloc[0:0]                                                          
    df_nivel_educativo = pd.concat([df_nivel_educativo, pd.DataFrame(nuevas_filas)])
    
    return df_nivel_educativo

def crear_tabla_poblacion(censo):
    """
    Devuelve un DataFrame con columnas:
    - id_depto
    - Depto (nombre del departamento)
    - Grupo_Etario (jardín, primaria, secundaria)
    - Cantidad (de personas en ese grupo etario)
    """

    # Inicializamos el DataFrame con sus respectivas columnas
    Poblacion = pd.DataFrame(columns=['id_depto', 'Depto', 'Grupo_Etario', 'Cantidad'])

    # Creamos un DataFrame temporal para acumular todas las edades de las comunas de CABA
    comuna_caba_acumulada = pd.DataFrame(columns=['Edad', 'Cantidad'])

    for fila in range(len(censo)):
        valor_columna = str(censo.loc[fila].iloc[1])  # Esta columna contiene "AREA" o las edades de cada departamento

        # Si la fila contiene "AREA", es el inicio de un nuevo bloque de departamento
        if "AREA" in valor_columna:
            dato_depto = valor_columna
            lista_dato_depto = dato_depto.split(" ")  # Separamos por espacios, quedaria asi ['AREA', '#', '86168']
            codigo_depto = int(lista_dato_depto[2])  # Agarraria '86168' y lo convierte a entero
            nombre_depto = str(censo.loc[fila].iloc[2])  # El nombre está en la tercera columna

            # Verificamos si es una comuna de CABA
            es_comuna_caba = 'Comuna' in nombre_depto

            # Caso especial para CABA: unificamos el código y nombre para todas las comunas
            if es_comuna_caba:
                codigo_depto = 2000
                nombre_depto = 'Ciudad Autónoma de Buenos Aires'

            # Inicializamos un DataFrame temporal con las edades de ese departamento
            df_edades = pd.DataFrame(columns=['Edad', 'Cantidad'])

        # Si la fila tiene una edad y una cantidad (no es nula y no es string), la agregamos al DataFrame temporal
        elif not pd.isna(censo.loc[fila].iloc[1]) and not isinstance(censo.loc[fila].iloc[1], str):
            edad = int(censo.loc[fila].iloc[1])
            cantidad = int(censo.loc[fila].iloc[2])
            df_edades.loc[len(df_edades)] = [edad, cantidad]

        # Si llegamos al final o a un nuevo "AREA", procesamos lo anterior
        if (fila + 1 == len(censo)) or ("AREA" in str(censo.loc[fila + 1].iloc[1])):
            if 'df_edades' in locals() and not df_edades.empty:
                if es_comuna_caba:
                    # Acumulamos las edades de las comunas de CABA en un solo DataFrame
                    comuna_caba_acumulada = pd.concat([comuna_caba_acumulada, df_edades])
                else:
                    # Agrupamos las edades en niveles educativos
                    df_niveles = separar_por_niveles(df_edades)

                    # Agregamos cada fila al DataFrame final
                    for _, row in df_niveles.iterrows():
                        Poblacion.loc[len(Poblacion)] = [
                            codigo_depto,
                            nombre_depto,
                            row['Grupo_Etario'],
                            row['Cantidad']
                        ]

    # Una vez procesadas todas las comunas de CABA, las agrupamos y agregamos como una sola entrada
    if not comuna_caba_acumulada.empty:
        # Sumamos las cantidades por edad
        comuna_caba_sumada = comuna_caba_acumulada.groupby('Edad', as_index=False).sum()

        # Agrupamos por niveles educativos
        df_niveles_caba = separar_por_niveles(comuna_caba_sumada)

        # Agregamos cada fila al DataFrame final como CABA unificada
        for _, row in df_niveles_caba.iterrows():
            Poblacion.loc[len(Poblacion)] = [
                2000,
                'Ciudad Autónoma de Buenos Aires',
                row['Grupo_Etario'],
                row['Cantidad']
            ]

    return Poblacion


# Creamos la tabla 
poblacion = crear_tabla_poblacion(censo)
# Modificamos estos id para que coincida con las otras tablas (explicado en el informe)
poblacion['id_depto'] = poblacion['id_depto'].replace({
    94008: 94007,
    94015: 94014
})

#%%
# TABLA EE (Establecimientos Educativos)

def crear_tabla_EE(EE):
    """
    Devuelve un DataFrame con las columnas:
    - Cueanexo
    - Nombre
    - id_depto 
    - Provincia (antes Jurisdicción)
    """

    # Nos quedamos solo con modalidad común
    EE = EE[EE['Común'] == 1].copy()

    # Función para obtener el ID del departamento 
    def obtener_id_depto(fila):
        if fila['Jurisdicción'] == 'Ciudad de Buenos Aires':
            return '2000'
        id_depto = str(fila['Código de localidad']).lstrip('0')
        if fila['Jurisdicción'] == 'Buenos Aires':
            return id_depto[:4]
        else:
            return id_depto[:5]

    # Aplicamos la función a cada fila
    EE['id_depto'] = EE.apply(obtener_id_depto, axis=1)

    # Convertimos a número para que sea más manejable 
    EE['id_depto'] = EE['id_depto'].astype(int)

    # Renombramos Jurisdicción a Provincia
    EE.rename(columns={'Jurisdicción': 'Provincia'}, inplace=True)
    
    # Renombramos la provincia ciudad de buenos aires para que coincida con las otras tablas
    EE['Provincia'] = EE['Provincia'].replace('Ciudad de Buenos Aires', 'Ciudad Autónoma de Buenos Aires')

    # Nos quedamos solo con las columnas necesarias
    EE = EE[['Cueanexo', 'Nombre', 'id_depto', 'Provincia']]

    return EE

# Creamos la tabla
ee = crear_tabla_EE(Establecimientos_educativos)

#%%
# TABLA BP (Bibliotecas populares)
"""
# Obtenemos los datos necesarios:
- fecha de fundación
- nombre
- id del departamento (lo renombramos como id_depto)
- mail
- provincia
"""
bp = dd.sql("""
    SELECT fecha_fundacion, nombre, id_departamento AS id_depto, mail, provincia
    FROM Bibliotecas_populares
""").df()

# Agregamos una columna 'Indice' que tiene números consecutivos desde 1
# Sirve como identificador único para cada biblioteca
bp.insert(0, 'Indice', range(1, len(bp) + 1))

# Me quedo solo con el dominio del mail
bp['dominio'] = bp['mail'].str.extract(r'@([a-zA-Z0-9\-]+)\.')
#Agregamos 'sin dominio' para que no tengamos nan
bp['dominio'] = bp['dominio'].fillna('sin_dominio')
# Ponemos todo en minúscula   
bp['dominio'] = bp['dominio'].str.lower() 

#Cambiamos valores de fecha_fundacion que aparecen como 'Nan' por 'sin fecha'
bp['fecha_fundacion'] = bp['fecha_fundacion'].fillna('sin fecha').astype(str)
#Borramos mail y nos quedamos unicamente con el dominio
bp = bp.drop(columns=['mail'])

#%%
# TABLA DEPARTAMENTO

def crear_tabla_departamento(poblacion, ee, bp):
    """
    Recibe los DataFrames: Poblacion, EE y BP.
    Devuelve un DataFrame con:
    - id_depto: código del departamento
    - Nombre: nombre del departamento
    - Provincia: provincia correspondiente
    """

    # Quitamos duplicados y renombramos columnas para que coincidan
    sin_repetidos_poblacion = poblacion[['id_depto', 'Depto']].drop_duplicates().rename(columns={'Depto': 'Nombre'})
    sin_repetidos_ee = ee[['id_depto', 'Provincia']].drop_duplicates()
    sin_repetidos_bp = bp[['id_depto', 'provincia']].drop_duplicates().rename(columns={'provincia': 'Provincia'})

    # Unimos EE y BP (info de provincias por id_depto)
    union_ee_bp = dd.sql("""
        SELECT * FROM sin_repetidos_ee
        UNION
        SELECT * FROM sin_repetidos_bp
    """).df()

    #eliminamos repetidos
    union_ee_bp_sin_repetidos = dd.sql("""
                    
                    SELECT DISTINCT * 
                    FROM union_ee_bp
                    """).df()
                    
    # Hacemos LEFT JOIN con población para agregar nombres y provincias
    departamento = dd.sql("""
        SELECT p.id_depto,
               p.Nombre,
               u.Provincia
        FROM sin_repetidos_poblacion AS p
        LEFT JOIN union_ee_bp_sin_repetidos AS u
          ON p.id_depto = u.id_depto
    """).df()
    # Agregamos la provincia del depto Tolhuin a mano ya que no se enceuntra en las otras tablas
    departamento['Provincia']=departamento['Provincia'].mask(departamento['id_depto']==94011, 'Tierra del Fuego')
    return departamento

# Creamos la tabla
departamento = crear_tabla_departamento(poblacion, ee, bp)

#%%
# Una vez que tenemos la tabla Departamento, ya no necesitamos que las tablas Poblacion, BP y EE 
# tengan la columna 'Provincia'o 'Depto' porque esa información ya está en Departamento.
# Nos quedamos solo con las columnas necesarias en cada tabla:

poblacion = poblacion[['id_depto', 'Grupo_Etario', 'Cantidad']]
bp = bp[['Indice', 'id_depto', 'fecha_fundacion', 'nombre', 'dominio']]
ee = ee[['Cueanexo', 'id_depto','Nombre']]

#%%
# TABLA NIVELES

niveles = pd.DataFrame({
    "Sector" : ["jardin", "primaria", "secundaria"]
    })

#%%
# TABLA ESTA_FORMADA_POR

def crear_esta_formada_por(Establecimientos_educativos):
    """
    Crea un DataFrame que relaciona cada escuela (Cueanexo)
    con los niveles educativos que ofrece: jardín, primario o secundario.
    """

    # Nos quedamos con las columnas necesarias
    tabla_ee = Establecimientos_educativos[[
        "Nivel inicial - Jardín maternal",
        "Nivel inicial - Jardín de infantes",
        "Primario",
        "Secundario",
        "Secundario - INET",
        "Cueanexo"
    ]]

    # Creamos el DataFrame que queremos devolver
    esta_formada_por = pd.DataFrame(columns=["Cueanexo_EE", "Sector_niveles"])

    # Recorremos fila por fila
    for i in range(len(tabla_ee)):
        cue = tabla_ee.loc[i, "Cueanexo"]

        # Si tiene jardín maternal o jardín de infantes, lo unificamos como "jardin"
        if tabla_ee.loc[i, "Nivel inicial - Jardín maternal"] == 1 or tabla_ee.loc[i, "Nivel inicial - Jardín de infantes"] == 1:
            esta_formada_por.loc[len(esta_formada_por)] = [cue, "jardin"]

        # Si tiene nivel primario
        if tabla_ee.loc[i, "Primario"] == 1:
            esta_formada_por.loc[len(esta_formada_por)] = [cue, "primario"]

        # Si tiene nivel secundario
        if tabla_ee.loc[i, "Secundario"] == 1 or tabla_ee.loc[i, "Secundario - INET"] == 1:
            esta_formada_por.loc[len(esta_formada_por)] = [cue, "secundario"]

    return esta_formada_por

# Creamos la tabla
esta_formada_por = crear_esta_formada_por(Establecimientos_educativos)


#%%
esta_formada_por.to_csv("TablasModelo/esta_formada_por.csv", index=False)
poblacion.to_csv("TablasModelo/poblacion.csv", index=False)
niveles.to_csv("TablasModelo/niveles.csv", index=False)
ee.to_csv("TablasModelo/ee.csv", index=False)
departamento.to_csv("TablasModelo/departamento.csv", index=False)
bp.to_csv("TablasModelo/bp.csv", index=False)

#%%
#                       CONSULTAS SQL
#%%
"""
i) Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de EE de cada nivel educativo, considerando solamente la
modalidad común, y la cantidad de habitantes por edad según los niveles
educativos. El orden del reporte debe ser alfabético por provincia y dentro de
las provincias, descendente por cantidad de escuelas primarias."""

def tabla_1():
    return dd.sql (""" 
                   SELECT 
                       d.Provincia,
                       d.Nombre AS Departamento,
                   
                       COUNT(CASE WHEN efp.Sector_niveles = 'jardin' THEN 1 END) AS Jardines,
                       MAX(CASE WHEN p.Grupo_Etario = 'jardin' THEN p.Cantidad ELSE 0 END) AS Poblacion_Jardin,
    
                       COUNT(CASE WHEN efp.Sector_niveles = 'primario' THEN 1 END) AS Primarias,
                       MAX(CASE WHEN p.Grupo_Etario = 'primaria' THEN p.Cantidad ELSE 0 END) AS Poblacion_Primaria,
    
                       COUNT(CASE WHEN efp.Sector_niveles = 'secundario' THEN 1 END) AS Secundarios,
                       MAX(CASE WHEN p.Grupo_Etario = 'secundaria' THEN p.Cantidad ELSE 0 END) AS Poblacion_Secundaria

                    FROM departamento AS d

                    LEFT JOIN ee ON ee.id_depto = d.id_depto
                    LEFT JOIN esta_formada_por AS efp ON efp.cueanexo_EE = ee.cueanexo
                    LEFT JOIN poblacion AS p ON p.id_depto = d.id_depto

                    GROUP BY d.Provincia, d.Nombre

                    ORDER BY d.Provincia ASC,
                    Primarias DESC;""").df()

consigna_1 = tabla_1()

#%%
"""
ii) Para cada departamento informar la provincia, el nombre del departamento y
la cantidad de BP fundadas desde 1950. El orden del reporte debe ser
alfabético por provincia y dentro de las provincias, descendente por cantidad
de BP de dicha capacidad."""

def tabla_2():
    return dd.sql("""
        SELECT 
            d.Provincia,
            d.Nombre AS Departamento,
            COUNT(CASE 
                WHEN bp.fecha_fundacion >= '1950-01-01' THEN 1 
                ELSE NULL 
            END) AS Cantidad_BP_Fundadas_Desde_1950
        FROM departamento d
        LEFT JOIN bp ON bp.id_depto = d.id_depto
        GROUP BY d.Provincia, d.Nombre
        ORDER BY d.Provincia ASC, Cantidad_BP_Fundadas_Desde_1950 DESC;
    """).df()
                    
consigna_2 = tabla_2()

#%%
"""
iii) Para cada departamento, indicar provincia, nombre del departamento,
cantidad de BP, cantidad de EE (de modalidad común) y población total.
Ordenar por cantidad EE descendente, cantidad BP descendente, nombre de
provincia ascendente y nombre de departamento ascendente. No omitir
casos sin BP o EE."""

def tabla_3():

    ee_por_depto = dd.sql("""
        SELECT id_depto, COUNT(*) AS Cantidad_EE
        FROM ee
        GROUP BY id_depto
    """).df()
    
    bp_por_depto = dd.sql("""
        SELECT id_depto, COUNT(*) AS Cantidad_BP
        FROM bp
        GROUP BY id_depto
    """).df()

    poblacion = dd.sql("""
        SELECT id_depto, MAX(Cantidad) AS Poblacion
        FROM poblacion
        WHERE Grupo_Etario = 'total'
        GROUP BY id_depto
    """).df()

    resultado = dd.sql("""
        SELECT 
            d.Provincia,
            d.Nombre AS Departamento,

            CASE WHEN ee.Cantidad_EE IS NULL THEN 0 ELSE ee.Cantidad_EE END AS Cantidad_EE,
            CASE WHEN bp.Cantidad_BP IS NULL THEN 0 ELSE bp.Cantidad_BP END AS Cantidad_BP,
            CASE WHEN p.Poblacion IS NULL THEN 0 ELSE p.Poblacion END AS Poblacion

        FROM departamento AS d

        LEFT JOIN bp_por_depto AS bp ON d.id_depto = bp.id_depto
        LEFT JOIN ee_por_depto AS ee ON d.id_depto = ee.id_depto
        LEFT JOIN poblacion AS p ON d.id_depto = p.id_depto

        ORDER BY 
            Cantidad_EE DESC,
            Cantidad_BP DESC,
            d.Provincia ASC,
            d.Nombre ASC
    """).df()

    return resultado

consigna_3 = tabla_3()
#%%
"""
iv) Para cada departamento, indicar provincia, el nombre del departamento y
qué dominios de mail se usan más para las BP"""

def tabla_4():
    dominio_por_depto = dd.sql("""
        SELECT 
            id_depto,
            dominio,
            COUNT(*) AS cantidad
        FROM bp
        GROUP BY id_depto, dominio
    """).df()
  
    maximos = dd.sql("""
        SELECT 
            d1.id_depto, 
            d1.dominio,
            d1.cantidad
        FROM dominio_por_depto AS d1
        WHERE d1.cantidad >= ALL (
            SELECT d2.cantidad
            FROM dominio_por_depto AS d2
            WHERE d1.id_depto = d2.id_depto
        )
    """).df()
    
    resultado = dd.sql("""
        SELECT 
            d.nombre AS Departamento,
            d.provincia AS Provincia,
            m.dominio AS Dominio_mas_frecuente_en_BP
        FROM maximos AS m
        LEFT JOIN departamento AS d
        ON m.id_depto = d.id_depto
        ORDER BY d.nombre, d.provincia
    """).df()
    
    return resultado

consigna_4 = tabla_4()

#%%
#                       VISUALIZACIONES
#%%
"""
CONSIGNA 1:
i) Cantidad de BP por provincia. 
Mostrarlos ordenados de manera decreciente por dicha cantidad.
"""
#Hacemos una consulta SQL que cuenta la cantidad de BP por provincia
def grafico1():
    bp_por_depto = dd.sql("""
        SELECT d.provincia, COUNT (*) as cantidad
        FROM departamento as d 
        LEFT JOIN bp
        ON d.id_depto = bp.id_depto
        GROUP BY d.provincia
        ORDER BY cantidad DESC
    """).df()
    return bp_por_depto

grafico1 = grafico1()

#Extraemos los datos del DataFrame para graficar
provincias = grafico1['Provincia']
cantidades = grafico1['cantidad']

#Invertimos el orden para que las barras con mas cantidad aparezcan arriba
provincias = provincias[::-1]
cantidades = cantidades[::-1]

#Creamos el gráfico
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(provincias, cantidades, color='darkturquoise')  # barras horizontales

#Ponemos etiquetas y formato
ax.set_xlabel('Cantidad de Bibliotecas Populares')
ax.set_ylabel('Provincia')
ax.set_title('Cantidad de BP por Provincia')
ax.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()

#%%
"""
CONSIGNA 2: 
ii) Graficar la cantidad de EE de los departamentos en función de la población,
separando por nivel educativo y su correspondiente grupo etario
(identificándolos por colores). Se pueden basar en la primera consulta SQL
para realizar este gráfico.
"""
# Creamos el gráfico a partir de la tabla de la consigna 1 de sql
fig, ax = plt.subplots()

# Graficamos los datos para jardin, primaria y secundaria
ax.scatter(data=consigna_1, x='Poblacion_Jardin', y='Jardines', s=6, color='red', alpha=0.3)
ax.scatter(data=consigna_1, x='Poblacion_Primaria', y='Primarias', s=6, color='darkturquoise', alpha=0.3)
ax.scatter(data=consigna_1, x='Poblacion_Secundaria', y='Secundarios', s=6, color='blue', alpha=0.2)

# se ajustan los límites
ax.set_xlim(0, 60000)
ax.set_ylim(0, 800)
ax.legend(["jardin", "primaria", "secundaria"], title="Niveles Educativos", loc='lower right', fontsize=10)

ax.set_title("Cantidad de EE de los departamentos \n en función de la población")
ax.set_xlabel("Cantidad de población por nivel educativo")
ax.set_ylabel("Cantidad de Establecimientos Educativos")

plt.show()


#%%%
"""
CONSIGNA 3:
iii) Realizar un boxplot por cada provincia, de la cantidad de EE por cada
departamento de la provincia. Mostrar todos los boxplots en una misma
figura, ordenados por la mediana de cada provincia.
"""
def grafico3():
    ee_por_depto = dd.sql("""
        SELECT d.provincia, d.Nombre AS depto ,COUNT (*) as cantidad
        FROM departamento as d 
        LEFT JOIN ee
        ON d.id_depto = ee.id_depto
        GROUP BY d.provincia, d.Nombre
        ORDER BY cantidad DESC
    """).df()
    return ee_por_depto

# Ejecutamos la función para obtener los datos
grafico3 = grafico3()

# Ordenamos las provincias según la mediana de EE por departamento
orden = grafico3.groupby('Provincia')['cantidad'].median().sort_values().index

# Crear lista de datos por provincia
datos = []
for provincia in orden:
    cantidades = grafico3[grafico3['Provincia'] == provincia]['cantidad'].values
    datos.append(cantidades)
 
# Graficamos los boxplots para cada provincia
plt.figure(figsize=(10, 6))
plt.boxplot(datos, labels=orden, patch_artist=True,
            boxprops=dict(facecolor='skyblue'),
            medianprops=dict(color='red'))
#Título y etiquetas de los ejes
plt.title('Distribución de EE por Departamento (por Provincia)')
plt.xlabel('Provincia')
plt.ylabel('Cantidad de EE por Departamento')

#Giramos las etiquetas del eje X para que se vean mejor
plt.xticks(rotation=45, ha='right') 

#Agregamos una grilla
plt.grid(True, linestyle='--', alpha=0.5) 
plt.show()

#%%
"""
CONSIGNA 4: 
iv) Relación entre la cantidad de BP cada mil habitantes y de EE cada mil
habitantes por departamento.
"""
# Hacemos una copia del DataFrame original para no modificarlo directamente
grafico4 = consigna_3.copy()

#Achicamos la poblacion para manejarla mejor
grafico4['Poblacion'] = grafico4['Poblacion']/1000

#Calculamos la cantidad de BP cada mil habitantes
grafico4['Cantidad_BP'] = grafico4['Cantidad_BP']/grafico4['Poblacion']

#Calculamos la cantidad de EE cada mil habitantes
grafico4['Cantidad_EE'] = grafico4['Cantidad_EE']/grafico4['Poblacion']

# Nos quedamos solo con las dos columnas que vamos a graficar
grafico4 = grafico4[['Cantidad_BP', 'Cantidad_EE']]

# Creamos el gráfico
fig, ax = plt.subplots()
ax.scatter(data=grafico4, x='Cantidad_BP', y='Cantidad_EE', s=3, color = "green", edgecolors='gray', linewidths=0.2)
ax.set_title("Relación entre la cantidad de BP y EE cada mil habitantes")
ax.set_xlabel("Cantidad de Biblotecas Populares (cada mil habitantes)")
ax.set_ylabel("Cantidad de Establecimientos Educativos \n (cada mil habitantes)");
   

