import csv
import pickle


def save_obj(obj, name):
    """
    This function save an object as a pickle.
    :param obj: object to save
    :param name: name of the pickle file.
    :return: -
    """
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
    """
    This function will load a pickle file
    :param name: name of the pickle file
    :return: loaded pickle file
    """
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)

def write_csv(data,outpath):
    with open(outpath +'/results.csv', 'w',newline='') as f:
        write = csv.writer(f)
        write.writerows(data)
    f.close()

def load_inverted_index(output_path = 'posting'):
    inverted_idx = load_obj(output_path + '/inverted_idx')
    return inverted_idx