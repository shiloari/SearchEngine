import json
import os
import traceback

import ranker
from indexer import Indexer
from parser_module import Parse
from ranker import Ranker
import utils
import numpy as np

class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index

    def getJSONfile(self, path):
        with open(path) as file:
            data = json.load(file)
            return data
        return None # File not found

    def getPostings(self):
        with open("./posting.json") as file:
            postings = json.load(file)
        return postings

    def MergeDocs(self, all_docs):
        from heapq import merge
        last = None
        for doc in merge(*all_docs):    ### remember *
            if doc != last:  # remove duplicates
                last = doc
                yield doc

    def relevant_docs_from_posting(self, query_as_dict):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        all_intersections = []
        for i in range(0, 3):
            matrix = np.zeros((len(query_as_dict.keys()), 2500000))
            for i, term in enumerate(query_as_dict):
                corrected_term = ranker.get_correct_term(term, self.inverted_index)
                np.put(matrix[i], self.inverted_index[corrected_term][1], 1)
            column_sums = matrix.sum(axis=0)
            intersection_docs = np.argwhere(column_sums >= 1).flatten()
            for i in range(2, len(query_as_dict.keys())):
                print('current intersection len: ', len(intersection_docs))
                new_intersection = np.argwhere(column_sums >= i).flatten()
                if len(new_intersection) < 2000:
                    break
                else:
                    intersection_docs = new_intersection
            all_intersections += intersection_docs
        print(len(all_intersections))
        return all_intersections

        # relevant_docs = {} ## {doc: {term1: df, term2: df}}
        # all_docs = []
        # for term in query_as_dict.keys():
        #     try: # an example of checks that you have to do
        #         #####
        #         # find in indexer -> posting file (json) -> dict(relevant docs): {doc_id:num_of_terms_relevant}
        #         correct_term = term.lower() if term.lower() in self.inverted_index.keys() else term.upper() \
        #             if term.upper() in self.inverted_index.keys() else None
        #         if correct_term is None:
        #             continue
        #         #print(self.inverted_index[correct_term][1])
        #         all_docs.append(self.inverted_index[correct_term][1])
        #     except:
        #         # traceback.print_exc()
        #         print('term {} not found in posting'.format(correct_term))
        # sorted_all_docs = list(self.MergeDocs(all_docs))
        # return sorted_all_docs
