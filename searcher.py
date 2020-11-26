import json
import os

from indexer import Indexer
from parser_module import Parse
from ranker import Ranker
import utils


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
        for doc in merge(all_docs):
            if doc != last:  # remove duplicates
                last = doc
                yield doc

    def relevant_docs_from_posting(self, query_as_list):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        relevant_docs = {} ## {doc: {term1: df, term2: df}}
        all_docs = []
        for term in query_as_list:
            try: # an example of checks that you have to do
                #####
                # find in indexer -> posting file (json) -> dict(relevant docs): {doc_id:num_of_terms_relevant}
                correct_term = term.lower() if term.lower() in self.inverted_index.keys() else term.upper() \
                    if term.upper() in self.inverted_index.keys() else None
                if correct_term is None:
                    continue
                all_docs += self.inverted_index[term][1]
            except:
                print('term {} not found in posting'.format(term))
        sorted_all_docs = list(self.MergeDocs(all_docs))
        return sorted_all_docs
