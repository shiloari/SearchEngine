import json
import time
import numpy as np
from numpy.linalg import norm


def get_correct_term(term, dictionary):
    """
    :param term: term to be checked
    :param dictionary: the dictionary which the term supposed to appear in some kind of way.
    :return:
    """
    l_term = term.lower()
    u_term = term.upper()
    corrected_term = l_term if l_term in dictionary.keys() else u_term if u_term in dictionary.keys() else None
    return corrected_term

class Ranker:
    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_doc(relevant_docs, query_as_dict, inverted_index, output_path, vectorDict):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """

        ranking_Dict = {}  # {doc: [[tweet_id, rank], [...]]}
        # compute the query's norm, not depended on the doc.
        norm_Q = norm(np.array(list(query_as_dict.values())))
        # for each doc: compute q*d, norm(d) - set the rank and add to the dictionary.
        for doc_id in relevant_docs:
            qd = 0
            for q_term in query_as_dict.keys(): # for: q*d (similarity)
                correted_term = get_correct_term(q_term, inverted_index)
                if correted_term in vectorDict[doc_id][0].keys():
                    qd += query_as_dict[q_term]*vectorDict[doc_id][0][correted_term]
            norm_D = norm(np.array(list(vectorDict[doc_id][0].values())))
            if norm_D == 0:
                ranking_Dict[doc_id] = [vectorDict[doc_id][1], 0]
            else:
                ranking_Dict[doc_id] = [vectorDict[doc_id][1], qd/(norm_D*norm_Q)]
        return ranking_Dict, sorted(ranking_Dict, key=lambda rank: ranking_Dict[rank][1], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
