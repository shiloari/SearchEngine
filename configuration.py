class ConfigClass:
    def __init__(self,corpus_path, save_in, stem):
        self.corpusPath = corpus_path
        self.savedFileMainFolder = save_in
        self.saveFilesWithStem = self.savedFileMainFolder + "/WithStem"
        self.saveFilesWithoutStem = self.savedFileMainFolder + "/WithoutStem"
        self.toStem = stem

    def get__corpusPath(self):
        return self.corpusPath
