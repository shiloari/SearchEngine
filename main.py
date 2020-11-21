import search_engine

if __name__ == '__main__':
    corpus_path = "C:/Users/gal/Desktop/Data"
    output_path = "C:/Users/gal/Desktop"
    queries = []
    numOfDocs = 20
    stemming = False
    search_engine.main(corpus_path, output_path, stemming, queries, numOfDocs)
