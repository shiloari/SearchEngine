import indexer
import ranker
import numpy as np


def update_relevant_terms(relevant_terms, query_as_dict, inverted_index, association_matrix):
    """"
    given list of relevant terms and the query,
    compute the Cij value from the inverted index.
    for each term in the query, pick the term from corpus
    which matched the most and expand the query.
    """
    all_terms = {}
    for y, term in enumerate(relevant_terms):
        all_terms[y] = term
        d_corrected_term = ranker.get_correct_term(term, inverted_index)
        for x, q_term in enumerate(query_as_dict):
            q_corrected_term = ranker.get_correct_term(q_term, inverted_index)
            if q_corrected_term is not None:
                Cij = len(np.intersect1d(inverted_index[q_corrected_term][1], inverted_index[d_corrected_term][1]))
                Cii = len(inverted_index[q_corrected_term][1])
                Cjj = len(inverted_index[d_corrected_term][1])
                association_matrix[x][y] = Cij/(Cii+Cjj-Cij)
    to_expand = []
    association_matrix = association_matrix.tolist()
    for x, term in enumerate(query_as_dict.keys()):
        sorted_list = sorted(association_matrix[x])
        if len(sorted_list) == 0:
            continue
        if len(sorted_list) <= 1:
            max_index = sorted_list[0]
        else:
            max_value = sorted_list[-2]
            max_index = association_matrix[x].index(max_value)
        #max_index = np.argpartition(association_matrix[x],range(len(association_matrix[x])))[-2]
        term_to_append = all_terms[max_index]
        if term_to_append == 'twitter.com': # Edge case of popular unimportant word
            sorted_list = sorted(association_matrix[x])
            if len(sorted_list) == 0:
                continue
            if len(sorted_list) <= 2:
                max_index = sorted_list[1]
            else:
                max_value = sorted_list[-3]
                max_index = association_matrix[x].index(max_value)
            #max_index = sorted(association_matrix[x])[-3]
            #max_index = np.argpartition(association_matrix[x],range(len(association_matrix[x])))[-3]
            term_to_append = all_terms[max_index]
        to_expand.append(term_to_append)
    return to_expand


def build_association_matrix(inverted_index, query_as_dict, top100, vectorDict):
    """

    :param inverted_index: the inverted index.
    :param query_as_dict: the query.
    :param top100: top 100 documents ID to expand.
    :param vectorDict: dictionary of all document vectors
    :return: the expanded query.
    """
    relevant_terms = set() # set of all unique terms from all top 100 docs
    #get all terms from the top 100 docs
    for doc_id in top100:
       relevant_terms = set(relevant_terms.union(vectorDict[doc_id][0].keys()))
    #set association matrix with the given shape
    association_matrix = np.zeros((len(query_as_dict.keys()), len(relevant_terms)))
    #find all most matching terms and expand the query
    expanded_query = update_relevant_terms(relevant_terms,query_as_dict,inverted_index, association_matrix)
    #update the value of appearances in the query.
    for term in expanded_query:
        if term in query_as_dict.keys():
            query_as_dict[term] += 1
        else:
            query_as_dict[term] = 1
    return query_as_dict