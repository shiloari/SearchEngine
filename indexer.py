import json
import os
import time
import traceback
import unicodedata
from threading import Thread
import threading
import copy

class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}  #{term:[total_frequency_in_corpus, unique_docs, path]}
        self.config = config
        if not os.path.isdir("./PostingFiles"):
            os.mkdir("./PostingFiles")
        self.outputPath = "./PostingFiles"
        self.postingDictionary = {} # {KEY: {term: [(docs,f),(doc,f)]}, {term: [(doc,f]}  }
        self.Locks = {}

    def flushAll(self):
       # keys = self.postingDictionary.keys()
        self.checkFlushing(0,0, True)

    def updateJSON(self, term, document_id, filename):
        # Get json data from path
        with open(self.outputPath + "/" + filename + ".json") as file:
            data = json.load(file)
        # get cluster by key (should choose how to represent the data)

        temp = data[term]  # Edit later
        # newData = {term: document_id}  # Add data here
        temp.append(document_id)  # append to cluster
        data.update({term: temp})
        # Write back to json file
        file = open(self.outputPath + '/' + filename + ".json", "a")
        json.dump(data, file, indent=4, sort_keys=True)
        file.close()

    def initializeJSON(self, term, document_id, filename):
        file = open(self.outputPath + "/" + filename + ".json", "a")
        data = {
            term: [
                document_id
            ]
        }
        json.dump(data, file, indent=4, sort_keys=True)
        file.close()

    def MergeDictionaries(self, data, key_data):
        for k in key_data.keys():
            if k not in data.keys():
                data[k] = key_data[k]
            else:
                data[k] += (key_data[k])
                data[k] = sorted(data[k], key=lambda x: x[0])
        return data

    def Flush(self, main_key, second_key, key_data, *args):
        # print('Start flush', key)
        startFlush = time.time()
        file = open(self.outputPath + "/" + main_key + '/' + second_key + ".json", "a")
        self.Locks[second_key].acquire()
        # while not file.closed:
        #     print("waiting...")
        #     with condition:
        #         condition.wait()
        if os.stat(self.outputPath + "/" +main_key + '/' + second_key + ".json").st_size != 0:
            # print('Read data from json')
            startRead = time.time()
            with open(self.outputPath + "/" + main_key + '/' + second_key + ".json") as file:
                data = json.load(file)
                data = self.MergeDictionaries(data, key_data)
                # print('Finish read and merge, time took is: ', time.time() - startRead)
                # file.close()
                with open(self.outputPath + "/" + main_key + '/' + second_key + ".json", 'w') as file:
                    json.dump(data, file, indent=4, sort_keys=True)
                    file.close()
        else:
            data = key_data
            json.dump(data, file, indent=4, sort_keys=True)
            file.close()
        self.Locks[second_key].release()

    def checkFlushing(self, main_key, second_key, FlushAll):
        if FlushAll or len(self.postingDictionary[main_key][second_key]) > 15000:
            numOfFlushed = 40
            threads = []
            # Append all threads to list
            # print("Start time: ", time.time())
            for m in sorted(self.postingDictionary, key=lambda m: len(self.postingDictionary[m]), reverse=True):
                for k in sorted(self.postingDictionary[m], key=lambda k: len(self.postingDictionary[m][k]), reverse=True):
                    if FlushAll or (numOfFlushed >= 0 and len(self.postingDictionary[m][k]) > 14000):
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
        document_id = document.tweet_id
        document_dictionary = document.term_doc_dictionary
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                ##################################
                # Set main and second keys for postings
                if self.keyIsGarbage(term[0]):
                    main_key = "Garbage"
                else:
                    main_key = term[0].lower()
                # elif term[0] == '@' or term[0] == '#':
                addition = '' if len(term) == 1 else "Garbage" if self.keyIsGarbage(term[1]) else term[1].lower()
                second_key = main_key + addition
                #################################
                # initialize postings directories, dictionaries
                if main_key not in self.postingDictionary.keys():
                    if not os.path.isfile(self.outputPath+"/"+main_key):
                        os.mkdir(self.outputPath+"/"+main_key) #"./posting/A
                    self.postingDictionary[main_key] = {}  # {A:{}}
                # update inverted index
                if second_key not in self.postingDictionary[main_key].keys():
                    self.postingDictionary[main_key][second_key] = {}
                    self.Locks[second_key] = threading.Lock()
                if term not in self.postingDictionary[main_key][second_key].keys():
                    self.postingDictionary[main_key][second_key][term] = [[document_id, document_dictionary[term]]]
                else:
                    self.postingDictionary[main_key][second_key][term].append([document_id, document_dictionary[term]])
                ###################################
                # initialize inverted index, update total appearances
                if term not in self.inverted_idx.keys():
                    # self.inverted_idx[term] = [document_dictionary[term], 1, self.outputPath+'/'+main_key+'/'+second_key]
                    self.inverted_idx[term] = 1
                    # self.inverted_idx[term][0] = document_dictionary[term]
                    # self.inverted_idx[term][1] = 1
                    # self.inverted_idx[term][2] = self.outputPath+'/'+main_key+'/'+second_key    # path
                else:
                    # self.inverted_idx[term][0] += document_dictionary[term]
                    self.inverted_idx[term]+= 1
                ###################################
                # Check if need to flush data to posting files
                self.checkFlushing(main_key, second_key, False)

            except:
                traceback.print_exc()
                print('problem with the following key {}'.format(term[0]))


