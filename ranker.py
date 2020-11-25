import json
import math


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
    def rank_relevant_doc(relevant_doc, query_as_list, inverted_index, output_path):
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
        ranking_Dict = {}  # {doc: [(term, tf_idf,cosSim), (term,tf_idf)]}
        # with open(output_path + "/CorpusSize.json") as file:
        with open("./PostingFiles/CorpusSize.json") as file:
            size_of_corpus = json.load(file)
            file.close()
        for term in query_as_list:
            ############
            #get correct term and pull data
            correct_term = term.lower() if term.lower() in inverted_index.keys() else term.upper() \
                if term.upper() in inverted_index.keys() else None
            path = inverted_index[correct_term][2]
            with open(path) as file:
                data = json.load(file)[correct_term]
                file.close()
            #############
            #compute tf_idf for each doc

            Wiq = 0.5 # SHOULD BE CHANGED!!
            dft = len(data)
            idf = math.log(size_of_corpus/dft, 10)
            for i in range(len(data)):
                tf = data[i][2]
                Wij += (math.pow(tf, 2))
                if data[i] not in ranking_Dict.keys():
                    ranking_Dict[data[i]] = [tf * idf, (tf*Wiq)*Wij/math.sqrt(math.pow(Wiq, 2))]
                else:
                    ranking_Dict[data[i]][0] += tf*idf
                    ranking_Dict[data[i]][1] += (tf*Wiq)/math.sqrt(math.pow(Wiq, 2))

        return sorted(ranking_Dict, key=lambda item: item[1]+item[2], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
