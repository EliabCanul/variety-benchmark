import pandas as pd
import numpy as np
import datetime 
import warnings


# Filtro para obtener solo eans numéricos
def is_int(x):
    """A filter to get only numeric values (eans)

    :param x: product identifier: ean, upc, gtin, etc.
    :type x: str
    :return: A flag saying if x can be converted tu numeric value
    :rtype: bool
    """
    try:
        xint= int(x)
        return True
    except:
        return False

def complete_13(x):
    """Function to add '0's to the left side of an string until
    complete 13 digits. Useful to match with EAN's digits

    :param x: product identifier: ean, upc, gtin, etc.
    :type x: str
    :return: an string with 13 digits to match eans.
    :rtype: str
    """
    sx = str(x)
    if len(sx)==13:
        return str(x)
    else:
        faltantes = 13 - len(sx)
        complete = "0"*faltantes + sx
        return complete
    
def rename_columns(store, df, logging):
    """Rename columns of raw dataframes. The changes are done depending 
    on the store

    :param store: store name
    :type store: str
    :param df: raw dataframe with columns to change
    :type df: dataframe
    :param logging: logger 
    :type logging: class
    :raises ValueError: Could not rename columns
    :return: dataframe with new column names
    :rtype: dataframe
    """
    
    if store == 'chedraui':
        df.rename(columns={'Input Category': 'input_category', 
                                'Category': 'category', 
                                'Subcategory 1': 'sub_category_1', 
                                'Subcategory 2': 'sub_category_2', 
                                'Product Name': 'product_name',
                                'UPC': 'upc',
                                'Normal Price': 'normal_price',
                                'URL': 'url'},
                    inplace=True)
        df=df[['upc','product_name', 'input_category', 'category', 'sub_category_1', 'sub_category_2', 'normal_price', 'url']]
        return df
        
    if store == 'aurrera':
        # changing category to subcategory
        df.rename(columns={'Category': 'subcategory', 
                                'EAN': 'ean',
                                'Product Name': 'product_name',
                                'Normal Price': 'normal_price',
                                'Current Price': 'current_price',
                                'URL': 'url'},
                    inplace=True)
        df = df[['ean','product_name', 'subcategory', 'normal_price', 'current_price','url']]
        return df
        
    # if ... another store
    
    else:
        #raise ValueError(f'Could not rename columns in dataframe for store {store}. ')
        logging.error(f'Could not rename columns in dataframe for store {store}. Check rename_columns in preprocessing.py')


def preprocess_df(store, df, logging):
    """ Preprocessing:       
        -change column names
        -take only eans with numeric values
        -verify 13 digits in ean

    :param store: store name
    :type store: str
    :param df: raw dataframe from scraper
    :type df: dataframe
    :param logging: logger
    :type logging: class
    :return: dataframe with adapted values for analysis
    :rtype: dataframe
    """
        
    # Rename columns conveniently 
    # NOTE: renaming columns is currently unnecessary (28/03/2022)
    # df = rename_columns(store, df, logging)

    if store == 'aurrera':
        df = df[ df['ean'].apply( lambda x: is_int(x)) ]
        df['ean'] = df['ean'].apply(lambda x: complete_13(x))
        # Add prefix
        ba_cols = [ "BA_"+c for c in df.columns.to_list()]
        df.columns = ba_cols
        
        # Category-subcategory mapping is currently unnecessary (28/03/2022)
        # since the scraped data in rds include them.
        """
        # Map subcategories to categories using ba_map_categories dictionary
        ba_subcat2cat= {}
        for k, v in ba_map_categories.items():
            for sub in v:
                ba_subcat2cat[sub] = k
        
        # Add category depending on subcategory
        df['BA_category'] = df['BA_subcategory'].apply(lambda x: ba_subcat2cat[x])
        """
        
        return df
    
    if store == 'chedraui':
        df = df[ df['upc'].apply(lambda x: is_int(x)) ]
        df['upc'] = df['upc'].apply(lambda x: complete_13(x))
        # Add prefix
        ch_cols = [ "CH_"+c for c in df.columns.to_list()]
        df.columns = ch_cols
        
        return df 
                
def say_if_its_in(x):
    """Convert default values after merge to explicitly
    say if the product is in 'neta', 'ambos' or 'BA'

    :param x: default label after merge
    :type x: str
    :return: _description_
    :rtype: _type_
    """
    
    if x=='right_only':
        return 'neta'
    if x=='both':
        return 'ambos'
    if x=='left_only':
        return 'BA'
    else:
        pass

def match_ba_categories(matching, not_matching):
    """A secondary function to match categories between BA and Neta

    :param matching: Dataframe with product coincidences between stores
    :type matching: dataframe
    :param not_matching: Dataframe with BA products not contained in Neta
    :type not_matching: dataframe
    """
    
    # matching = yes_from_aurrera
    match_categories_BA = matching.groupby('NETA_category')['BA_category'].agg('unique')

    dict_categories_BA = dict(zip(match_categories_BA.index, [list(x) for x in match_categories_BA] ))

    for k, v in dict_categories_BA.items():
        
        print("Neta: ", k)
        print("  BA: ", ", ".join(v))
        print()

    # lista de categorías de BA que (SI) tienen relación con categorías en Neta
    flat_categories_in_BA = [item for sublist in [list(x) for x in match_categories_BA] for item in sublist] 

    print("\n".join(flat_categories_in_BA))

    # Categorías de BA que NO aparecen en Neta
    for cat in not_matching.BA_category.unique():
        if cat in flat_categories_in_BA:
            # La categoria si existe
            pass
        else:
            print(cat)
    return

# Dictionary mapping categories with subcategories.
# Extracted manually from BA webpage
ba_map_categories = {
    
    'Electrónica y Computación': [
        'Audífonos y bocinas',
        'Computación',
        'Fotografía',
        'Pantallas',
        'Smartphones',
        'Videojuegos'
    ],
    'Hogar y Accesorios': [
        'Accesorios para cocina',
        'Accesorios para mesa',
        'Artículos deportivos',
        'Artículos de temporada',
        'Artículos para fiesta',
        'Artículos para Jardinería',
        'Blancos y Colchones',
        'Campismo y Natación',
        'Electrodomésticos',
        'Ferretería y accesorios para Autos',
        'Línea Blanca',
        'Motos & Accesorios para Moto',
        'Muebles y Decoración',
        'Organización y Almacenamiento',
        'Papelería',
        'Pilas y baterías',
        'Pintura'
    ],
    'Despensa y abarrotes': [
        'Aceites de Cocina',
        'Alimentos Instantáneos',
        'Alimentos Saludables',
        'Azúcar y sustitutos',
        'Botanas',
        'Café, té y Sustitutos de crema',
        'Cereales y Barras energéticas',
        'Chocolates y Dulces',
        'Comida Oriental',
        'Enlatados y Conservas',
        'Frijol, arroz y semillas',
        'Galletas dulces y saladas',
        'Harina y polvo para hornear',
        'Jarabes Saborizantes',
        'Leche',
        'Miel y Mermelada',
        'Pan y Tortillas Empacados',
        'Pastas y sopas',
        'Salsas y Aderezos',
        'Sazonadores y Especias'
    ],
    'Carnes, Mariscos y Pescados': [
        'Carne de Cerdo',
        'Carne de Res',
        'Pescados y Mariscos',
        'Pollo y Pavo'
    ],
    'Vinos, Licores y Cerveza': [
        'Digestivos',
        'Coolers',
        'Cervezas',
        'Licores',
        'Vinos'
    ],
    'Ropa y Zapatería': [
        'Accesorios casual',
        'Ropa para Hombre',
        'Ropa para Mujer',
        'Ropa para Niña y Juvenil',
        'Ropa para Niño y Juvenil'
    ],
    'Bebidas y Jugos': [
        'Agua embotellada',
        'Bebidas Energizantes e isotónicas',
        'Café y Té Preparado',
        'Néctares y Jugos',
        'Polvos y Jarabes saborizantes',
        'Refrescos'
    ],
    'Congelados': [
        'Comida congelada',
        'Frutas y Verduras Congeladas',
        'Hielo',
        'Postres Congelados'
    ],
    'Mascotas y Limpieza': [
        'Accesorios para Limpieza',
        'Alimento y accesorios para Mascotas',
        'Aromatizantes para el hogar',
        'Artículos de lavandería',
        'Jabón para Lavatrastes y lavavajillas',
        'Limpieza del hogar',
        'Papel higiénico',
        'Platos, Vasos y Cubiertos Desechables'
    ],
    'Tortillería y Panadería': [
        'Comida preparada',
        'Pan Dulce',
        'Pan Salado',
        'Pastelería y Repostería',
        'Tortillería'
    ],
    'Lácteos y Cremería': [
        'Alimento Líquido',
        'Crema',
        'Gelatinas y Postres preparados',
        'Huevo',
        'Leche',
        'Mantequilla y margarina',
        'Yogurt'
    ],
    'Artículos para Bebés y Niños': [
        'Artículos deportivos para bebé',
        'Comida para bebé y leche materna',
        'Cunas, carriolas y accesorios',
        'Fórmula láctea',
        'Higiene del bebé',
        'Juguetería',
        'Juguetes para bebé',
        'Pañales y toallitas para bebé',
        'Ropa para bebé'
    ],
    'Salchichonería': [
        'Artículos Empacados',
        'Carnes frías',
        'Quesos'
    ],
    'Frutas y Verduras': [
        'Artículos a granel',
        'Frutas',
        'Verduras'
    ],
    'Farmacia': [
        'Alimentación correcta',
        'Analgésicos',
        'Bienestar Sexual',
        'Cuidado Personal',
        'Incontinencia',
        'Material de Curación',
        'Medicamentos de Patente',
        'Medicamentos Estomacales',
        'Medicamentos Naturistas y Herbolarios',
        'Medicamentos Oftálmicos y Ópticos',
        'Medicamentos Respiratorios',
        'Ortopedia y Equipos de Medición',
        'Para la Diabetes',
        'Vitaminas y Suplementos'
    ],
    'Higiene Personal y Belleza': [
        'Artículos para Afeitar',
        'Cuidado bucal',
        'Cuidado del cabello',
        'Cuidado facial',
        'Cuidado Íntimo',
        'Cuidado para pies',
        'Higiene corporal',
        'Higiene para hombre',
        'Higiene para manos',
        'Kits de viaje',
        'Maquillaje y cosméticos',
        'Pañuelos desechables'
    ]
    
}