# Game of Thrones Indexing Coding Challenge

# Problem
Searching information in huge books collection can be painful. Therefore, the challenge here is to build an inverted index of the documents to speed up calculation.

# Assumption
1. Typo of a word, such as game and game, will count as two different words.
2. Assume all documents can be processed in memory.
3. 

# Solution
The plan here is to use Spark to batch process these documents. The documents will be converted into list of words. Then, spark will remove any duplicate values. For example, if the word "use" appears multiple times in a document, Spark will remove all the duplicate words. Then, Spark will aggregate all documents name with the word appeared into the same list. Then, the dictionary with all the unique words and indices will be generated. The dictionary will be used to replace all words with indices. Both dictionary and simiplfied reversed index lists will be sent as output.

# Future Plan
Currently, the dictionary is updated based on all the input texts to reduce lookup operations. In the future, we might want to use an existing dictionary and compare if words are already in the list and append to existing reversed index lists. If that was the case, dictionary and existing reversed index lists need to be imported before the entire process.