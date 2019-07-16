from pyspark import SparkContext
from pyspark.sql import SQLContext
from  pyspark.sql.functions import input_file_name
import string 
import os

def cleanWords(words):
    try:
        words = words.encode('utc-8')
    except:
        print("The file is already in utc-8 format")

    words = words.lower()
    cleaned_words = words.translate(str.maketrans('', '', string.punctuation))
    return cleaned_words

def processFile(path, filename, sc, rddList):
    text_file = sc.textFile(path + filename)
    words_rdd = text_file.flatMap(lambda line: cleanWords(line).split())\
        .filter(lambda x: len(x) > 0)
    words_rdd = words_rdd.map(lambda x: (x, filename)).distinct()
    
    rddList.append(words_rdd)

def main(inputFolder, outputFolder):
    sc = SparkContext.getOrCreate()
    rddList = []

    # Read text files
    for doc in os.listdir(inputFolder):
        # Clean File
        try:
            processFile(inputFolder, doc, sc, rddList)
        except:
            print("The filename must be integers.")
    
    # Combine RDD into 1
    try:
        combined_rdds = sc.union(rddList)\
            .map(lambda x: (x[0], [int(x[1])]))\
            .reduceByKey(lambda x,y: x+y)
    except:
        print("There are no files in input folder")

    # Create Index for dictionary
    word_dict = combined_rdds.map(lambda x: x[0]).zipWithIndex()
    word_dict.saveAsTextFile(outputFolder + 'dictionary')
    
    # Convert to index values
    word_dict = word_dict.collectAsMap()
    combined_rdds = combined_rdds.map(lambda x: (word_dict[x[0]], x[1]))

    # Output Files
    combined_rdds.saveAsTextFile(outputFolder + 'reverse_index')
    

if __name__ == '__main__':
    inputFolder = './input/indexing/'
    outputFolder = './output/'
    main(inputFolder, outputFolder)