import json
import math


def readData(key_num, output_path):
    with open(output_path + "/" + str(key_num) + ".json", 'r') as file:
        data = json.load(file)
        file.close()
    return data


class Ranker:
    def __init__(self):
        self.CosSimDict = {}

    def CosSim(self,query, relevant_docs):
        CosSimRanked = {}
                # Tqw = len(term)  # set formula
                # Tdw =
                # CosSimRanked[doc] +=
        pass


    @staticmethod
    def rank_relevant_doc(relevant_docs, query_as_list, inverted_index, output_path):
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
        with open(output_path+"/wordCorpusSize.json") as file:
            num_of_words_in_corpus = json.load(file)
            file.close()
        ###compute normaQ
        queryDict = {}
        for term in query_as_list:
            if term not in queryDict:
                queryDict[term] = 1
            else:
                queryDict[term] += 1
        normaQ = 0
        for term in queryDict:
            normaQ += math.pow(queryDict[term],2)
        key_num = int(relevant_docs[0] / 100000)
        data = readData(key_num, output_path)
        ####for each doc in relevant docs
        for i in range(0, len(relevant_docs)):
            # first case scenario
            if relevant_docs[i] > (key_num+1)*100000:    #should get new data, update key_num
                key_num += 1
                data = readData(key_num, output_path)
            doc_len = data[i][1]
            doc_dictionary = data[i][3]
            ### compute the norma of doc
            normaD = 0
            for val in doc_dictionary.value():
                normaD += math.pow(val,2)
            QD = 0
            for term in query_as_list:
                ############
                l_term = term.lower()
                u_term = term.upper()
                corrected_term = l_term if l_term in doc_dictionary.keys() else u_term if u_term in doc_dictionary.keys() else None
                #compute tf_idf
                tf = doc_dictionary[corrected_term]/doc_len
                idf = math.log(num_of_words_in_corpus,10)
                tf_idf = tf*idf
                ### compute cosSim
                q = queryDict[term]
                d = 0 if corrected_term is None else doc_dictionary[corrected_term]
                QD += q*d*tf_idf
            cosSim = QD/math.sqrt(normaQ*normaD)
            ranking_Dict[relevant_docs[i]] = cosSim
        return sorted(ranking_Dict, key=lambda rank: ranking_Dict[rank], reverse=True)


    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
