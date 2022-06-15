# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
# from dotenv import find_dotenv, load_dotenv
from multiprocessing import Pool
from itertools import repeat
import json

@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
#    load_dotenv(find_dotenv())

    main()


class file_utilities:
    """file handling function"""

    def get_keys_from_prefix(self, bucket):
        '''gets list of keys and dates for given bucket and prefix'''
        keys_list = []
        paginator = s3.get_paginator('list_objects_v2')
        # use Delimiter to limit search to that level of hierarchy
        for page in tqdm(paginator.paginate(Bucket=bucket), total=1200):
            keys = [content['Key'] for content in page.get('Contents')]
            # print('keys in page: ', len(keys))
            keys = [key for key in keys if 'json' in key]
            keys_list.extend(keys)
        return keys_list

    def process_study_json(self, bucket, key):
        """read json file"""
        data_stream = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
        progress_bar.update(1)
        if "CONSTITUCION" in data_stream[:200]:
            data = json.loads(data_stream)
            dictout = get_data_study(data)
            return dictout

        return None


def find_item(struct, field='tipo', string=''):
    for item in struct:
        if item[field] == string:
            return item
    return None


def get_data_study(dj):
    """
    Parses the relevant data from a JSON

    inputs
    ------

    dj: json
        data jason

    outputs
    -------

    dict
        With the relevant columns
    """

    parsed = {}

    # others = ['MODIFICACION', 'TRANSFORMACION', 'DISOLUCION', 'EMIGRACION',
    #           'MIGRACION', 'SANEAMIENTO', 'RECTIFICACION', 'ANOTACION']
    tipo_actuacion = dj['tipo_actuacion']
    if tipo_actuacion == 'CONSTITUCION':
        parsed = get_data_constitucion(dj)
    else:
        parsed = get_data_modificacion(dj)
    return parsed


def get_data_constitucion(dj):
    """Gets data in case it's a constitucion

    Parameters
    ----------
    dj : dict
        JSON dictionary

    Returns
    -------
    dict
        Organized with relevant data
    """

    parsed = {}
    parsed['rut'] = dj['rut']
    parsed['cve'] = dj['cve']
    parsed['fecha'] = dj['fecha']
    parsed['actuacion'] = dj['tipo_actuacion']
    parsed['lugar'] = find_item(dj['estructura'][1]['articulos'],
                                field='articulo',
                                string='inicio')['texto']
    parsed['tipo_sociedad'] = dj['tipo_sociedad']

    parsed['objeto'] = find_item(dj['estructura'][1]['articulos'],
                                 field='subtitulo',
                                 string='OBJETO')['texto']

    parsed['firmas'] = find_item(dj['estructura'], 
                                 field='tipo',
                                 string='firmas')['firmas']
    return parsed


def get_data_modificacion(dj):
    """Gets data in case it's a constitucion

    Parameters
    ----------
    dj : dict
        JSON dictionary

    Returns
    -------
    dict
        Organized with relevant data
    """

    parsed = {}
    parsed['rut'] = dj['rut']
    parsed['cve'] = dj['cve']
    parsed['fecha'] = dj['fecha']
    parsed['actuacion'] = dj['tipo_actuacion']
    parsed['lugar'] = None
    parsed['tipo_sociedad'] = None

    parsed['objeto'] = None

    parsed['firmas'] = None
    return parsed


def get_dflist_multiprocess(keys_list, progress_bar, num_proc=4):
    with Pool(num_proc) as pool:
        df_list = pool.starmap(fu.process_study_json, zip(repeat(bucket), keys_list), 15)
        pool.close()
        pool.join()
    return df_list
