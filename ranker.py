class Ranker:
    def __init__(self):
        pass

    def CosSim(self,query, relevant_docs):
        CosSimRanked = {}
        for doc in relevant_docs.keys():
            for term in query:
                Tqw = len(term)  # set formula
                Tdw =
                CosSimRanked[doc] +=
    @staticmethod
    def rank_relevant_doc(relevant_doc):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers solely the number of terms shared by the tweet (full_text) and query.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ######
        # rank all relevant docs - tf idf + improvements?
        # get the top K ranked docs - K = 100? 1000?
        # use Local Method - built matrix.
        # return sorted


        return sorted(relevant_doc.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]
