import os
import time
import unicodedata
import numpy as np

import indexer
import local_method
import ranker
from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import glob
from pathlib import Path
import json


def saveAsJSON(path, file_name, to_be_saved,how_to):
    file = open(path + "/" + file_name + ".json", how_to)
    json.dump(to_be_saved, file, indent=4, sort_keys=True)
    file.close()


def clearSingleEntities(inv_index, parser, output_path):
    EntitiesDict = {}       #{doc_id: [term1,term2]}
    docs_to_clear = {}      # {json0: [doc1 ,doc2]}
    terms_to_be_removed = []
    for term in inv_index.keys():
        if parser.isEntity(term) and len(inv_index[term][1]) == 1:
            if inv_index[term][1][0] in EntitiesDict.keys():
                EntitiesDict[inv_index[term][1][0]].append(term)
            else:
                EntitiesDict[inv_index[term][1][0]] = [term]
            terms_to_be_removed.append(term)
    for term in terms_to_be_removed:
        inv_index.pop(term)
    if len(EntitiesDict.keys()) == 0:
        return
    sorted_keys = sorted(EntitiesDict.keys())
    key_num = int(sorted_keys[0]/indexer.jsonSize)
    docs_to_clear[key_num] = []
    for doc_id in sorted_keys:
        if doc_id >= (key_num + 1) * indexer.jsonSize:  # should get new data, update key_num
            key_num = int(doc_id/indexer.jsonSize)
            docs_to_clear[key_num] = [doc_id]
        else:
            docs_to_clear[key_num] += [doc_id]

    num_of_cleared_in_corpus = 0
    for json_key in docs_to_clear.keys():
        data = ranker.readData(json_key, output_path+ '/PostingFiles')
        for doc_id in data.keys():
            doc_idstr = str(doc_id)
            if doc_id in EntitiesDict.keys():
                for entity in EntitiesDict[doc_id]:
                    data[doc_idstr][1] -= data[doc_idstr][3][entity]
                    data[doc_idstr][3].pop(entity)
            values = data[doc_idstr][3].values()
            if len(values) != 0:
                d_vector = np.array(values)
                data[doc_idstr][2] = max(values)
            else:
                data[doc_idstr][2] = 0
                d_vector = np.array([0])
        saveAsJSON(output_path + '/PostingFiles', str(json_key), data,"w")

def run_engine(corpus_path, output_path, stemming):
    """

    :return:
    """
    number_of_documents = 0

    config = ConfigClass()
    r = ReadFile(corpus_path=corpus_path)
    p = Parse(stemming)
    indexer = Indexer(output_path)
    globList = []

    # folders = 0
    # for _, dirnames, _ in os.walk(corpus_path):
    #     # ^ this idiom means "we won't be using this value"
    #     folders += len(dirnames)
    # progressBar = ''
    # for i in range(folders):
    #     progressBar += ' '
    # progressBar = '[' + progressBar + ']'
    # print(progressBar, ' 0%')
    startCorpus = time.time()
    start = time.time()
    sizeOfCorpus = 0
    for path in Path(corpus_path).rglob('*.parquet'):
        parsingTime = 0
        indexingTime = 0
        print("New Document")
        # if sizeOfCorpus == 1:
        #     break
        print("start parse parquet")
        start1 = time.time()
        counter = 0
        counter2 = 0
        for idx, document in enumerate(r.read_file(file_name=path)):
                # if sizeOfCorpus < 7 :
                #     counter += 1
                #     continue
                # if counter <150000:
                #     counter += 1
                #     continue
                # if counter > 24999:
                #     print("parsed 25000 files in average time: ", (time.time()-start1)/25000)
                #     counter = 0
                #     start1 = time.time()
                # print(idx)
                # parse the document
                startParse = time.time()
                parsed_document = p.parse_doc(document)
                parsingTime += time.time() - startParse
                number_of_documents += 1
                # index the document data
                startIndex = time.time()
                indexer.add_new_doc(parsed_document)
                indexingTime += time.time() - startIndex
                counter += 1
                counter2 += 1
        print('-------------------------------------------------------------')
        print("Time to whole parse parquet: " + str(time.time() - start))
        print("Average time to parse tweet: " + str((time.time() - start)/counter2))
        print('Parsing total time: ', parsingTime, ' | Indexing total time: ', indexingTime)
        print('-------------------------------------------------------------')
        start = time.time()
        sizeOfCorpus += 1
        # progressBar = progressBar[:idx] + '\x1b[6;30;42m' + 'X' + '\x1b[0m]' + progressBar[idx:]
        # print(progressBar, ' ',  float(counter/folders),' %', end='\r')
    if(sizeOfCorpus == 0):
        with open(indexer.outputPath + "/inverted_idx_save.json",'r') as file:
            x = json.load(file)
            file.close()
    #indexer.inverted_idx = x.copy()
    print("End and start to full flush !")
    start22 = time.time()
    indexer.Flush(indexer.json_key,indexer.postingDictionary)
    indexer.WriteCorpusSize()
    print("Total time to Flush: ",time.time() - start22)
    print("Total time to parse and index: ", time.time()-startCorpus)
    #### save as json

    print('Finished parsing and indexing. Starting to export files')
    start22 = time.time()
    clearSingleEntities(indexer.inverted_idx, p, output_path)
    print("Total time to clear entities: ", time.time() - start22)
    start22 = time.time()
    saveAsJSON('.', 'inverted_idx', indexer.inverted_idx,"a")
    print("Total time to write index: ", time.time() - start22)
    # saveAsJSON('.', 'posting' , indexer.postingDictionary)
    # utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    start = time.time()
    # inverted_index = ("inverted_idx")
    with open("C:/Users/gal/Desktop/inverted_idx.json") as file:
        inverted_idx = json.load(file)
    print('Finito! : ',time.time() -start)
    return inverted_idx


def search_and_rank_query(query, inverted_index, k, output_path):
    p = Parse()
    start = time.time()
    query_as_dict = p.parse_sentence(query, term_dict={})
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_dict)
    ranked_docs, sorted_keys = searcher.ranker.rank_relevant_doc(relevant_docs, query_as_dict, inverted_index, output_path)  # { doc: 4, doc: 10}
    top_100_keys = searcher.ranker.retrieve_top_k(sorted_keys, 100)
    # expanded_query = local_method.build_association_matrix(inverted_index, query_as_dict, top_100_keys, output_path)
    # ranked_docs, sorted_keys = searcher.ranker.rank_relevant_doc(relevant_docs, expanded_query, inverted_index, output_path)  # { doc: 4, doc: 10}
    # top_k_keys = searcher.ranker.retrieve_top_k(sorted_keys, k)
    top_K = []
    for doc_id in top_100_keys:
        top_K.append(ranked_docs[doc_id])
    print ("finish : ",time.time() -start)
    return top_K

def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    #run_engine(corpus_path, output_path, stemming)
    query = input("Please enter a query: ")
    num_docs_to_retrieve = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    conter = 0
    print(len(inverted_index))
    for teem in inverted_index.keys():
        start = time.time()
        print(inverted_index["trump"][0])
        print(time.time()-start)
        conter += 1
    for doc_tuple in search_and_rank_query(query, inverted_index, num_docs_to_retrieve, output_path+"/PostingFiles"):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
