import math
import os
import time
import unicodedata
import numpy as np
from numpy.linalg import norm
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

def updateVectorsFile(doc_id, data, inv_index, num_of_docs_in_corpus, vectorsDict):   #termsData = {term: [[doc_id, tweet_id, d, |d|]]}
    values = list(data[3].values())
    #sigmaTfidf =0
    vectorsDict[doc_id] = [{}, data[0]]
   # vectorsDict[doc_id][0] = {} #all terms in doc and tf_idf
    #vectorsDict[doc_id][1] = data[0]    #tweet id
    for term in data[3].keys():
        corrected_term = ranker.get_correct_term(term, inv_index)
        tf = data[3][term]/len(values)
        idf = math.log(num_of_docs_in_corpus / len(inv_index[corrected_term][1]), 2)
        tf_idf = tf*idf
        vectorsDict[doc_id][0][corrected_term] = tf_idf
        # sigmaTfidf += math.pow(tf_idf, 2)
    # norma_d = math.sqrt(sigmaTfidf)
    # v_norma_d = np.array(values)
    # norma_d = norm(v_norma_d)

def clearSingleEntities(inv_index, parser, output_path, num_of_docs_in_corpus):
    EntitiesDict = {}       #{doc_id: [term1,term2]}
    docs_to_clear = {}      # {pkl_id: [doc1 ,doc2]}
    vectorsDict = {}
    for term in inv_index.keys():
        if inv_index[term][0] == 1:
            single_doc = inv_index[term][1][0]
            if single_doc in EntitiesDict.keys():
                EntitiesDict[single_doc].append(term)
            else:
                EntitiesDict[single_doc] = [term]
    if len(EntitiesDict.keys()) == 0:
        return
    sorted_keys = sorted(EntitiesDict.keys())   # all docs to clear
    key_num = int(sorted_keys[0]/indexer.jsonSize)
    docs_to_clear[key_num] = []
    for doc_id in sorted_keys:
        if doc_id >= (key_num + 1) * indexer.jsonSize:  # should get new data, update key_num
            key_num = int(doc_id/indexer.jsonSize)
            docs_to_clear[key_num] = [doc_id]
        else:
            docs_to_clear[key_num] += [doc_id]

    num_of_cleared_in_corpus = 0
    for pkl_key in docs_to_clear.keys():
        data = utils.load_obj(output_path + '/PostingFiles/' + str(pkl_key))
        #data = ranker.readData(json_key, output_path+ '/PostingFiles')
        counter = 0
        for doc_id in data.keys(): #key is now a string
            doc_idint = int(doc_id)
            if doc_idint in EntitiesDict.keys():
                for entity in EntitiesDict[doc_idint]:
                    if len(data[doc_id][3]) >= 5 or parser.isEntity(entity):
                        data[doc_id][1] -= data[doc_id][3][entity]
                        data[doc_id][3].pop(entity)
                        inv_index.pop(entity)
                        if counter == 0:
                            print(entity)
                            print(entity in inv_index.keys())
                            counter += 1
            values = data[doc_id][3].values()
            if len(values) != 0:
                data[doc_id][2] = max(values)
            else:
                data[doc_id][2] = 0
            updateVectorsFile(doc_id, data[doc_id], inv_index, num_of_docs_in_corpus, vectorsDict)
        utils.save_obj(data, output_path + '/PostingFiles/' + str(pkl_key))
    utils.save_obj(vectorsDict, output_path + '/PostingFiles/vectorsFile')
    vectorsDict.clear()
    docs_to_clear.clear()
    EntitiesDict.clear()
        # saveAsJSON(output_path + '/PostingFiles', str(json_key), data,"w")


def FlushTermsData(output_path, termsData, indexer):
    to_be_flushed = {}
    sorted_terms = sorted(termsData)
    main_key = sorted_terms[0][0].lower()
    if not (main_key.isalpha() or main_key.isnumeric() or main_key == '@' or main_key == '#'):
        main_key = "Garbage"
    # print(main_key)
    for term in sorted_terms:
        l_term =term[0].lower()
        if l_term != main_key:
            utils.save_obj(to_be_flushed, output_path + '/' + main_key)
            #indexer.Flush(main_key, to_be_flushed, output_path)
            to_be_flushed = {}
            if not (l_term.isalpha() or l_term.isnumeric() or l_term == '@' or l_term == '#'):
                # print('key before change: ', main_key)
                main_key = "Garbage"
            else:
                main_key = l_term
            # print(main_key)
        to_be_flushed[term] = termsData[term]
    if len(to_be_flushed.keys()) != 0:
        #indexer.Flush(main_key, to_be_flushed, output_path)
        utils.save_obj(to_be_flushed, output_path + '/' + main_key)


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
        if sizeOfCorpus == 1:
            break
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
    # if(sizeOfCorpus == 0):
    #     with open(indexer.outputPath + "/inverted_idx_save.json",'r') as file:
    #         x = json.load(file)
    #         file.close()
    #indexer.inverted_idx = x.copy()
    print("End and start to full flush !")
    start22 = time.time()
    #indexer.Flush(indexer.json_key,indexer.postingDictionary, indexer.postingsPath)
    utils.save_obj(indexer.postingDictionary, indexer.postingsPath + '/' + str(indexer.json_key))
    #indexer.WriteCorpusSize()
    print("Total time to Flush: ",time.time() - start22)
    print("Total time to parse and index: ", time.time()-startCorpus)
    #### save as json

    print('Finished parsing and indexing. Starting to export files')
    start22 = time.time()
    clearSingleEntities(indexer.inverted_idx, p, output_path, indexer.num_of_docs_in_corpus)
    # if not os.path.isdir(indexer.postingsPath + "/TermsData"):
    #     os.mkdir(indexer.postingsPath+ "/TermsData")
    # FlushTermsData(indexer.postingsPath + "/TermsData", indexer.termsData,indexer)
    print("Total time to clear entities: ", time.time() - start22)
    start22 = time.time()
    #saveAsJSON('.', 'inverted_idx', indexer.inverted_idx,"a")
    utils.save_obj(indexer.inverted_idx, output_path + '/inverted_idx')
    indexer.inverted_idx.clear()
    print('inverted index is empty: ' , len(indexer.inverted_idx.keys()) == 0)
    print("Total time to write index: ", time.time() - start22)
    # saveAsJSON('.', 'posting' , indexer.postingDictionary)
    # utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # utils.save_obj(indexer.postingDict, "posting")


def load_index(output_path):
    print('Load inverted index')
    start = time.time()
    # inverted_index = ("inverted_idx")
    inverted_idx = utils.load_obj(output_path + '/inverted_idx')
    # with open(output_path + "/inverted_idx.json") as file:
    #     inverted_idx = json.load(file)
    print('Finito! : ',time.time() -start)
    return inverted_idx


def search_and_rank_query(query, inverted_index, k, output_path, vectorDict):
    p = Parse()
    start = time.time()
    query_as_dict = p.parse_sentence(query, term_dict={})
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_dict)
    start_rank = time.time()
    ranked_docs, sorted_keys = searcher.ranker.rank_relevant_doc(relevant_docs, query_as_dict, inverted_index, output_path, vectorDict)  # { doc: 4, doc: 10}
    print('end rank ', time.time()-start_rank)
    top_100_keys = searcher.ranker.retrieve_top_k(sorted_keys, 100)
    matrix_start = time.time()
    expanded_query = local_method.build_association_matrix(inverted_index, query_as_dict, top_100_keys, output_path, vectorDict)
    print('matrix time ', time.time()-matrix_start)
    start_rank = time.time()
    ranked_docs, sorted_keys = searcher.ranker.rank_relevant_doc(relevant_docs, expanded_query, inverted_index, output_path, vectorDict)  # { doc: 4, doc: 10}
    print('end rank ', time.time()-start_rank)
    top_k_keys = searcher.ranker.retrieve_top_k(sorted_keys, k)
    top_K = []
    for doc_id in top_100_keys:
        top_K.append(ranked_docs[doc_id])
    print ("finish : ",time.time() -start)
    return top_K

def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    #run_engine(corpus_path, output_path, stemming)
    vectorsFile = utils.load_obj(output_path + '/PostingFiles/vectorsFile')
    inverted_index = load_index(output_path)
    query = input("Please enter a query: ")
    num_docs_to_retrieve = int(input("Please enter number of docs to retrieve: "))
    for doc_tuple in search_and_rank_query(query, inverted_index, num_docs_to_retrieve, output_path+"/PostingFiles", vectorsFile):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
