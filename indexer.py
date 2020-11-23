import json
import os
import traceback


class Indexer:

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        if not os.path.isdir("./PostingFiles"):
            os.mkdir("./PostingFiles")
        self.outputPath = "./PostingFiles"
        self.KeyDict = {}

    def hashTerm(self, term):
        if term[0].isalpha():
            pass
        elif term[0].isnumeric():
            pass
        else:
            pass

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

    def MergeDictionaries(self, data, key):
        for k in self.KeyDict[key].keys():
            if k not in data.keys():
                data[k] = self.KeyDict[key][k]
            else:
                data[k] += (self.KeyDict[key][k])
                data[k] = sorted(data[k], key=lambda x: x[0])
        return data

    def checkFlushing(self, key):
        if len(self.KeyDict[key]) > 1000:
            file = open(self.outputPath + "/" + key + ".json", "a")
            if os.stat(self.outputPath + "/" + key + ".json").st_size != 0:
                with open(self.outputPath + "/" + key + ".json") as file:
                    data = json.load(file)
                    data = self.MergeDictionaries(data, key)
                    # file.close()
                    with open(self.outputPath + "/" + key + ".json", 'w') as file:
                        json.dump(data, file, indent=4, sort_keys=True)
                        file.close()
            else:
                data = self.KeyDict[key]
                json.dump(data, file, indent=4, sort_keys=True)
                file.close()
                # file = open(self.outputPath + "/" + key + ".json", "a")
                # json.dump(self.KeyDict[key], file, indent=4, sort_keys=True)
                # file.close()

                # file.close()
            self.KeyDict[key] = {}

    def keyIsGarbage(self, key):
        ordKey = ord(key)
        return ordKey == 34 or ordKey == 42 or ordKey == 47 or ordKey == 58 or ordKey == 60 \
            or ordKey == 62 or ordKey == 63 or ordKey == 92 or ordKey == 124

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
                if self.keyIsGarbage(term[0]):
                    key = "Garbage"
                else:
                    key = term[0].lower()
                if key not in self.KeyDict:
                    self.KeyDict[key] = {}
                # Update inverted index and posting
                if term not in self.inverted_idx.keys():
                    self.inverted_idx[term] = 1
                    self.postingDict[term] = []
                    # self.initializeJSON(term, document_id, filename=key)
                else:
                    self.inverted_idx[term] += 1
                    # self.updateJSON(term, document_id, key)
                if term not in self.KeyDict[key].keys():
                    self.KeyDict[key][term] = [[document_id, document_dictionary[term]]]
                else:
                    self.KeyDict[key][term].append([document_id,document_dictionary[term]])
                self.checkFlushing(key)
                # json_file_name = term # should change. hash func?
            # self.postingDict[term].append((document.tweet_id, json_file_name))

            except:
                traceback.print_exc()
                print('problem with the following key {}'.format(term[0]))
