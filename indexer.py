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

    def MergeDictionaries(self, data, key_data):
        for k in key_data.keys():
            if k not in data.keys():
                data[k] = key_data[k]
            else:
                data[k] += (key_data[k])
                data[k] = sorted(data[k], key=lambda x: x[0])
        return data

    def Flush(self, main_key, data, output_path):
        if len(data) != 0:
            # with open(output_path + "/" + str(main_key) + ".json", 'w+') as file:
            #     json.dump(data, file, indent=4, sort_keys=True)
            #     file.close()
            utils.save_obj(data,output_path + "/" + str(main_key))


    def checkFlushing(self, main_key, second_key, FlushAll):
        if FlushAll or len(self.postingDictionary[main_key][second_key]) > 20000:
            numOfFlushed = 40
            threads = []
            # Append all threads to list
            # print("Start time: ", time.time())
            for m in sorted(self.postingDictionary, key=lambda m: len(self.postingDictionary[m]), reverse=True):
                for k in sorted(self.postingDictionary[m], key=lambda k: len(self.postingDictionary[m][k]), reverse=True):
                    if FlushAll or (numOfFlushed >= 0 and len(self.postingDictionary[m][k]) > 18000):
                        copyData = copy.deepcopy(self.postingDictionary[m][k])
                        t = Thread(target=self.Flush, args=(m, k, copyData, ''))
                        self.postingDictionary[m][k] = {}  # a -> ax {key:[values]} = {}
                        threads.append(t)
                        # print("start thread: ", key, ' ,', len(copyData))
                        t.start()
                        numOfFlushed -= 1
                    else:
                        break
                # for thread in threads:
                #     thread.join()

    def keyIsGarbage(self, key):
        ordKey = ord(key)
        return ordKey == 34 or ordKey == 42 or ordKey == 46 or ordKey == 47 or ordKey == 58 or ordKey == 60 \
            or ordKey == 62 or ordKey == 63 or ordKey == 92 or ordKey == 124 or ordKey == 10 or ordKey == 13 \
            or unicodedata.category(key) == 'Lo'



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
                self.inverted_idx[term][1][document_id] = [tweet_id, None, None]
            else:
                # term is upper but lower in inverted index
                if l_term in self.inverted_idx.keys():
                    self.inverted_idx[l_term][0] += document_dictionary[term]
                    self.inverted_idx[l_term][1][document_id] = [tweet_id, None, None]
                    # self.inverted_idx[l_term][0] += document_dictionary[term]
                    # self.inverted_idx[l_term][1] += [document_id]
                # term is lower but upper in inverted index
                elif u_term in self.inverted_idx.keys():
                    self.inverted_idx[term] = self.inverted_idx[u_term]
                    self.inverted_idx[term][1][document_id] = [tweet_id, None, None]
                    self.inverted_idx[term][0] += document_dictionary[term]
                    # self.inverted_idx[term] = [self.inverted_idx[u_term][0] + document_dictionary[term],
                    #                            self.inverted_idx[u_term][1] + [document_id]]
                    self.inverted_idx.pop(u_term)
                # none exists in inverted index. append.
                else:
                    self.inverted_idx[term] =[document_dictionary[term], {document_id: [tweet_id, None, None]}]
        # Flush if needed
        if len(self.postingDictionary.keys()) > jsonSize-1:
            #threading.Thread(target=self.Flush, args=(self.json_key, self.postingDictionary.copy())).start()
            #self.Flush(self.json_key,self.postingDictionary, self.postingsPath)
            utils.save_obj(self.postingDictionary, self.postingsPath + '/' + str(self.json_key))
            self.postingDictionary.clear()




