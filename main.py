import search_engine

if __name__ == '__main__':
    corpus_path = "C:/Users/shilo/Desktop/Data"
    output_path = "C:/Users/shilo/Desktop"
    queries = []
    numOfDocs = 20
    stemming = False
    search_engine.main(corpus_path, output_path, stemming, queries, numOfDocs)
