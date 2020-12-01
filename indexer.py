import json
import os
import time
import traceback
import unicodedata
from threading import Thread
import threading
import copy

import utils

jsonSize = 100000


class Indexer:

    def __init__(self, output_path):
        self.inverted_idx = {}  #{term:[total_frequency_in_corpus, [doc_id, doc_id, ...]]}
        self.outputPath = output_path
        if not os.path.isdir(output_path+ "/PostingFiles"):
            os.mkdir(output_path+"/PostingFiles")
        self.postingsPath = self.outputPath+"/PostingFiles"
        self.postingDictionary = {} #  {doc_id: [tweet_id, doc_len, max_f, term_dict]}  }
        self.Locks = {}
        self.num_of_docs_in_corpus = 0
        self.json_key = 0
        self.termsData = {}

    # def flushAll(self):
    #    se

    def WriteCorpusSize(self):
        file = open(self.postingsPath + "/CorpusSize.json", "a")
        json.dump(self.num_of_docs_in_corpus, file, indent=4)
        file.close()


    def Flush(self, main_key, data, output_path):
        if len(data) != 0:
            # with open(output_path + "/" + str(main_key) + ".json", 'w+') as file:
            #     json.dump(data, file, indent=4, sort_keys=True)
            #     file.close()
            utils.save_obj(data,output_path + "/" + str(main_key))



    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        # Exctract data, updata vairables

        document_id = document.doc_id
        tweet_id = document.tweet_id
        document_dictionary = document.term_doc_dictionary
        self.json_key = int(int(document_id)/jsonSize)  ################ set the best hash func
        # Go over each term in the doc
        num_values = sum(document_dictionary.values())
        self.num_of_docs_in_corpus += 1
        max_f = 0
        if len(document_dictionary.values()) >0:
            max_f = max(document_dictionary.values())
        self.postingDictionary[document_id] = [tweet_id, num_values, max_f, document_dictionary]

       # index each term in document dictionary
        for term in document_dictionary:
            l_term = term.lower()
            u_term = term.upper()
            if term in self.inverted_idx.keys():
                self.inverted_idx[term][0] += document_dictionary[term]
                self.inverted_idx[term][1] += [document_id]
            else:
                # term is upper but lower in inverted index
                if l_term in self.inverted_idx.keys():
                    # self.inverted_idx[l_term][0] += document_dictionary[term]
                    # self.inverted_idx[l_term][1][document_id] = [tweet_id, None, None]
                    self.inverted_idx[l_term][0] += document_dictionary[term]
                    self.inverted_idx[l_term][1] += [document_id]
                # term is lower but upper in inverted index
                elif u_term in self.inverted_idx.keys():
                    # self.inverted_idx[term] = self.inverted_idx[u_term]
                    # self.inverted_idx[term][1][document_id] = [tweet_id, None, None]
                    # self.inverted_idx[term][0] += document_dictionary[term]
                    self.inverted_idx[term] = [self.inverted_idx[u_term][0] + document_dictionary[term],
                                               self.inverted_idx[u_term][1] + [document_id]]
                    self.inverted_idx.pop(u_term)
                # none exists in inverted index. append.
                else:
                    self.inverted_idx[term] = [document_dictionary[term], [document_id]]
        # Flush if needed
        if len(self.postingDictionary.keys()) > jsonSize-1:
            #threading.Thread(target=self.Flush, args=(self.json_key, self.postingDictionary.copy())).start()
            #self.Flush(self.json_key,self.postingDictionary, self.postingsPath)
            utils.save_obj(self.postingDictionary, self.postingsPath + '/' + str(self.json_key))
            self.postingDictionary.clear()




