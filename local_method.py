import ranker
import numpy as np


def update_relevant_terms(dictionary, relevant_terms, query_as_dict, inverted_index):
    for term in dictionary:
        if term in relevant_terms.keys():
            continue
        relevant_terms[term] = 0
        for q_term in query_as_dict:
            l_term = q_term.lower()
            u_term = q_term.upper()
            corrected_term = l_term if l_term in inverted_index.keys() else u_term if u_term in inverted_index.keys() else None
            if corrected_term is not None:
                relevant_terms[term] += len(np.intersect1d(inverted_index[corrected_term][1], inverted_index[corrected_term][1]))

def build_association_matrix(inverted_index, query_as_dict, top100, output_path):
    not_in_RAM = [] # {term: [doc1, doc2]}
    relevant_terms = {}
    for doc_id in top100:
        if not doc_id in ranker.Ranker.topCosSim.keys():
            not_in_RAM.append(doc_id)
        else:
            update_relevant_terms(ranker.Ranker.topCosSim[doc_id], relevant_terms, query_as_dict, inverted_index)
    if len(not_in_RAM) == 0:
        return

    not_in_RAM = sorted(not_in_RAM)
    key_num = int(not_in_RAM[0] / 100000)
    data = ranker.readData(key_num, output_path)
    for doc_id in not_in_RAM:
        if doc_id >= (key_num + 1) * 100000:  # should get new data, update key_num
            key_num = int(doc_id / 100000)
            data = ranker.readData(key_num, output_path)
        doc_dictionary = data[str(doc_id)][3]
        update_relevant_terms(doc_dictionary, relevant_terms, query_as_dict, inverted_index)

    relevant_keys = sorted(relevant_terms.keys(), key=relevant_terms.get, reverse=True)
    relevant_counter = 0
    dict_counter = len(query_as_dict.keys())
    while relevant_counter < dict_counter:
        if relevant_keys[relevant_counter] not in query_as_dict.keys():
            query_as_dict[relevant_keys[relevant_counter]] = 1
        else:
            dict_counter += 1
        relevant_counter += 1
    return query_as_dict