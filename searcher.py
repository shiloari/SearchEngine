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

    def getJSONfile(self, key):
        if Indexer.keyIsGarbage(key):
            key = "Garbage"
        file = open(self.outputPath + "/" + key + ".json", "a")
        if os.stat(self.outputPath + "/" + key + ".json").st_size != 0:
            with open(self.outputPath + "/" + key + ".json") as file:
                data = json.load(file)

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
                # find in indexer -> posting file (json) -> list(relevant docs)
                self.getJSONfile(term[0])  #get by term[0]

                #####
                posting_doc = posting[term]
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = 1
                    else:
                        relevant_docs[doc] += 1
            except:
                print('term {} not found in posting'.format(term))
        return relevant_docs
