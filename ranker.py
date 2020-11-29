import json
import math
import numpy as np
from numpy.linalg import norm

def readData(key_num, output_path):
    with open(output_path + "/" + str(key_num) + ".json", 'r') as file:
        data = json.load(file)
        file.close()
    return data


class Ranker:
    topCosSim = {}
    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_doc(relevant_docs, query_as_dict, inverted_index, output_path):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ######
        # rank all relevant docs - F(tf idf , CosSim, improvements?) = rank
        # get the top K ranked docs - K = 100? 1000?
        # use Local Method - built matrix.
        # return sorted
        ranking_Dict = {}  # {doc: [(tf_idf,cosSim), (term,tf_idf)]}
        # with open(output_path + "/CorpusSize.json") as file:
        with open(output_path+"/CorpusSize.json") as file:
            num_of_docs_in_corpus = json.load(file)
            file.close()

        key_num = int(relevant_docs[0] / 100000)
        data = readData(key_num, output_path)
        ####for each doc in relevant docs
        fixed_query_dict = {}
        for term in query_as_dict.keys():
            fixed_query_dict[term.lower()] = query_as_dict[term]
        for doc_id in relevant_docs:
            q_Vector = []
            d_Vector = []
            if doc_id >= (key_num + 1) * 100000:  # should get new data, update key_num
                key_num = int(doc_id / 100000)
                data = readData(key_num, output_path)
            doc_dictionary = data[str(doc_id)][3]
            tweet_id = data[str(doc_id)][0]
            for term in doc_dictionary.keys():
                l_term = term.lower()
                u_term = term.upper()
                corrected_term = l_term if l_term in inverted_index.keys() else u_term if u_term in inverted_index.keys() else None
                l_correct = corrected_term.lower()
                if l_correct not in fixed_query_dict.keys():
                    q_Vector.append(0)
                else:
                    q_Vector.append(fixed_query_dict[l_correct])
                dict_len = sum(doc_dictionary.values())
                val = doc_dictionary[term]
                tf = val/dict_len
                idf = math.log(num_of_docs_in_corpus / len(inverted_index[corrected_term][1]), 2)
                tf_idf = tf * idf
                d_Vector.append(val*tf_idf)
            q_Vector_np = np.array(q_Vector)
            d_Vector_np = np.array(d_Vector)
            cos_sim = np.dot(q_Vector_np, np.transpose(d_Vector_np)) / (norm(q_Vector_np) * norm(d_Vector_np))
            if cos_sim > 0.7 and len(Ranker.topCosSim.keys()) <= 100000:
                Ranker.topCosSim[doc_id] = doc_dictionary
            ranking_Dict[doc_id] = (tweet_id, cos_sim)
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
