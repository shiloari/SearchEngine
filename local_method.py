import indexer
import ranker
import numpy as np


def update_relevant_terms(relevant_terms, query_as_dict, inverted_index, association_matrix):
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
    for x in range(len(query_as_dict.keys())):
        maxX = association_matrix[x].max()
        # print(np.where(association_matrix[x] == maxX))
        # print(np.where(association_matrix[x] == maxX)[0][0])
        max_index= np.where(association_matrix[x] == maxX)[0][0]
        term_to_append = all_terms[max_index]
        # print(term_to_append)
        to_expand.append(term_to_append)

    return to_expand


def build_association_matrix(inverted_index, query_as_dict, top100, output_path, vectorDict):
    #not_in_RAM = [] # {term: [doc1, doc2]}
    relevant_terms = set() # set of all unique terms from all top 100 docs
    for doc_id in top100:
       relevant_terms =  set(relevant_terms.union(vectorDict[doc_id][0].keys()))
    print(len(query_as_dict.keys()), ' ', len(relevant_terms))
    association_matrix = np.zeros((len(query_as_dict.keys()), len(relevant_terms)))
    expanded_query = update_relevant_terms(relevant_terms,query_as_dict,inverted_index, association_matrix)
    for term in expanded_query:
        if term in query_as_dict.keys():
            query_as_dict[term] += 1
        else:
            query_as_dict[term] = 1
    return query_as_dict
    #
    #     terms = vectorDict[doc_id].keys()
    #
    #
    #
    #     for term in terms:
    #         if term in relevant_terms:
    #             continue
    #         else:
    #             update_relevant_terms(,relevant_terms,)
    #     if not doc_id in ranker.Ranker.topCosSim.keys():
    #         not_in_RAM.append(doc_id)
    #     else:
    #         update_relevant_terms(ranker.Ranker.topCosSim[doc_id], relevant_terms, query_as_dict, inverted_index)
    # if len(not_in_RAM) == 0:
    #     return
    # print('num of docs not in ram ', len(not_in_RAM))
    # not_in_RAM = sorted(not_in_RAM)
    # key_num = int(not_in_RAM[0] / indexer.jsonSize)
    # data = ranker.readData(key_num, output_path)
    # for doc_id in not_in_RAM:
    #     if doc_id >= (key_num + 1) * indexer.jsonSize:  # should get new data, update key_num
    #         key_num = int(doc_id / indexer.jsonSize)
    #         data = ranker.readData(key_num, output_path)
    #     doc_dictionary = data[str(doc_id)][3]
    #     update_relevant_terms(doc_dictionary, relevant_terms, query_as_dict, inverted_index)
    #
    # relevant_keys = sorted(relevant_terms.keys(), key=relevant_terms.get, reverse=True)
    # relevant_counter = 0
    # dict_counter = len(query_as_dict.keys())
    # while relevant_counter < dict_counter:
    #     if relevant_keys[relevant_counter] not in query_as_dict.keys():
    #         query_as_dict[relevant_keys[relevant_counter]] = 1
    #     else:
    #         dict_counter += 1
    #     relevant_counter += 1
    # return query_as_dict