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

    def getKeyJSONfile(self, key):
        if Indexer.keyIsGarbage(key):
            key = "Garbage"
            with open("./PostingFiles/" + key + ".json") as file:
                data = json.load(file)
                return data
        else:
            return None # File not found

    def getPostings(self):
        with open("./posting.json") as file:
            postings = json.load(file)
        return postings

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        relevant_docs = {}
        for term in query:
            try: # an example of checks that you have to do
                #####
                # find in indexer -> posting file (json) -> dict(relevant docs): {doc_id:num_of_terms_relevant}
                if term not in self.inverted_index:
                    continue     #### should handle?
                postings = self.getPostings()
                key = term[0] if len(term) == 1 else 'Garbage' if Indexer.keyIsGarbage(term[0]) else term[0]+'Garbage' \
                    if Indexer.keyIsGarbage(term[1]) else term[:2]
                posting_doc = postings[key][term]    # {term: (doc_id,f)}
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = 1
                    else:
                        relevant_docs[doc] += 1
            except:
                print('term {} not found in posting'.format(term))
        return relevant_docs
