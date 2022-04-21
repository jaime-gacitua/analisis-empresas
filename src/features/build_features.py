import re
from tqdm.notebook import tqdm


def remove_stop_phrases(corpus):
    """Removes a set of phrases that are very common
    in the text and reduce the power of topic modelling.

    Parameters
    ----------
    corpus : list of str
        each document as an element of a list

    Returns
    -----------

    list of str
        with all the common phrases removed
    """

    REMOVE = ['la empresa tendra por objeto desarrollar las siguientes actividades:',
              'la sociedad tendra por objeto desarrollar las siguientes actividades:',
               'la actividad economica que constituira el objeto o el giro de la empresa sera :',
               'el la sociedad tiene por objeto la',
               'el objeto de la sociedad sera:',
               'el objeto de la empresa sera:',
               'el objeto de es:',
               'objeto de la sociedad sera',
               'el objeto de la sociedad es',
               'la sociedad tendra como objeto unico',
               'prestacion de',
               'al por',
               'el objeto de la empresa sera',
               'objeto social',
               'actividad',
               'actividades',
               'cualquier otra',
               'la sociedad tiene por objeto',
               'en general',
               'clase de',
               'cuenta propia',
               'y de',
               'todo tipo',
               'toda',
               'clase',
               'relacionadas con',
               'socios',
               'podra',
               'realizar',
               'la sociedad',
               'realizacion',
               'otras']

    corpus_out = corpus.copy()
    for s in tqdm(REMOVE):
        s = ' ' + s + ' '
        corpus_out = [re.sub(s, ' ', ' ' + x + ' ').strip() for x in corpus_out]

    return corpus_out
