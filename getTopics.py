from flask import Flask, request, stream_with_context
from keyphrase_vectorizers import KeyphraseCountVectorizer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise  import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import DBSCAN
from nltk import pos_tag
from nltk.stem.porter import PorterStemmer
from nltk.util import ngrams
from nltk import word_tokenize
from operator import itemgetter
import itertools
import numpy as np
import pandas as pd
import csv
import re
#import nltk
#nltk.download()

model = SentenceTransformer('distilbert-base-nli-mean-tokens', device='cpu')
vectorizer = KeyphraseCountVectorizer()
stemmer = PorterStemmer()
pattern = re.compile('[0-9%]+')

app = Flask(__name__)

def standardiseTopics(topics):
    inputToOutput = [
        {'input':'order', 'output':'orders'},
        {'input':'competition', 'output':'competition'},
        {'input':'customers', 'output':'customers'},
        {'input':'cash', 'output':'cashflow'},
        {'input':'exports', 'output':'exports'},
        {'input':'backlog', 'output':'order backlog'}
    ]

    allTopics = [topic for row in topics for topic in row['topics']]
    outputs = []
    for topic in allTopics:
        output = {'input':topic, 'output':topic}
        tokens = [stemmer.stem(token) for token in word_tokenize(topic)]
        for _token in inputToOutput:
            if stemmer.stem(_token['input']) in tokens:
                output['output']=_token['output']

        outputs.append(output)

    stemmed_outputs=[]
    for output in outputs:
        stemmed_output = ' '.join([stemmer.stem(token) for token in word_tokenize(output['output'])])
        matches = [{'i':i, 'x':x} for i, x in enumerate(stemmed_outputs) if x['stem']==stemmed_output]
        if len(matches)>0:
            inputs = stemmed_outputs[matches[0]['i']]['inputs']
            inputs.append(output['output'])
            if len(matches[0]['x']['actual'])<len(output['output']):
                stemmed_outputs[matches[0]['i']]={'stem':stemmed_output, 'actual':output['output']}
            stemmed_outputs[matches[0]['i']]['inputs']=inputs
        else:
            stemmed_outputs.append({'stem':stemmed_output, 'actual':output['output'], 'inputs':[output['output']]})

    newTopics = []

    for row in topics:
        outputs = []
        for topic in row['topics']:
            for item in stemmed_outputs:
                if topic in item['inputs']:
                    if item['actual'] not in outputs:
                        outputs.append(item['actual'])
                    break

        newTopics.append({'id':row['id'], 'topics':outputs})

    return newTopics


def getIgnoreList():
    with open("./ignoreList.csv", 'r') as file:
        csv_reader = csv.reader(file)
        ignoreList = [stemmer.stem(row[0]) if len(word_tokenize(row[0]))==1 else row[0] for row in csv_reader]
        return ignoreList

def allOkayToAdd(phrase, ignoreList):
    if len(word_tokenize(phrase))>3:
        return False

    if len(pattern.findall(phrase))>0:
        return False

    for ignoreWord in ignoreList:
        n = len(ignoreWord.split(' '))
        checkWords = [' '.join(ngram).lower() for ngram in ngrams(word_tokenize(phrase), n)]
        for checkWord in checkWords:
            if checkWord==ignoreWord:
                return False

    for token in phrase.split(' '):
        stemmedToken = stemmer.stem(token)
        if stemmedToken in ignoreList:
            return False

    return True

def preprocess(text):
    text = text.replace('-', ' ')
    return text

#https://maartengr.github.io/KeyBERT/api/mmr.html - maximal marginal relevance
def mmr(doc_embedding, candidates, candidate_embeddings, max_topics=10, diversity=0.7):
    all_candidate_doc_similarity = cosine_similarity(candidate_embeddings,[doc_embedding])
    all_candidate_candidate_similarity = cosine_similarity(candidate_embeddings)

    #seed the first keyword - the one most similar to doc
    keywords_idx = [np.argmax(all_candidate_doc_similarity)]
    candidates_idx = [i for i in range(len(candidates)) if i not in keywords_idx]

    for _ in range(min(max_topics-1, len(candidates)-1)):
        #rank similarity of remaining candidates to doc
        candidate_doc_similarity = all_candidate_doc_similarity[candidates_idx,:]

        #rank similarity of remaining candidates to already selected keywords and calculate the maximum similarity of each of them to keywords
        candidate_keyword_similarity = np.max(all_candidate_candidate_similarity[candidates_idx][:,keywords_idx],axis=1)


        mmr = (1 - diversity) * candidate_doc_similarity - diversity * candidate_keyword_similarity.reshape(-1, 1)
        mmr_idx = candidates_idx[np.argmax(mmr)]
        keywords_idx.append(mmr_idx)
        candidates_idx.remove(mmr_idx)

    keywords = [
        (candidates[idx], round(float(all_candidate_doc_similarity.reshape(1,-1)[0][idx]),4))
        for idx in keywords_idx
    ]

    keywords = sorted(keywords, key=itemgetter(1), reverse=True)
    return keywords

def mss(doc_embedding, candidates, candidate_embeddings):
    all_candidate_doc_similarity = cosine_similarity([doc_embedding], candidate_embeddings)
    all_candidate_candidate_similarity = cosine_similarity(candidate_embeddings)

    max_n = 20
    candidates_idx = all_candidate_doc_similarity.argsort()[0][-max_n:]


    upto = 5
    combination = None
    min_sum = np.inf
    #get all combinations of candiates - and calculate the
    for combination_idx in itertools.combinations(candidates_idx, upto):
        _sum = sum([all_candidate_candidate_similarity[i][j] for i in combination_idx for j in combination_idx if i!=j])

        if _sum<min_sum:
            min_sum = _sum
            combination = candidates[list(combination_idx)]

    print (combination)

#topics=[{id:, topics:}]
def combineSimilarTopics(docs, topic_data):
    all_topics = np.unique([topic[0] for row in topic_data for topic in row['topics']])

    topic_lengths = [len(word_tokenize(topic)) for topic in all_topics]
    topic_count_vectorizer = CountVectorizer(ngram_range=(min(topic_lengths), max(topic_lengths)), vocabulary=all_topics)
    topic_counts = topic_count_vectorizer.fit_transform(docs).toarray()

    all_topics_embedding = model.encode(all_topics)
    all_topics_similarity = cosine_similarity(all_topics_embedding)

    all_topics_similarity_threshold = np.where(all_topics_similarity>0.9, 0, 1)
    clustering = DBSCAN(min_samples=2, eps=0.1).fit(all_topics_similarity_threshold)

    #use pandas to get clusters quickly
    labels = pd.DataFrame(clustering.labels_)
    labels['idx']=np.arange(labels.shape[0])

    #labels = labels.T
    clusters_df=labels.groupby(by=0).agg({'idx':lambda series:' '.join([str(x) for x in series])})
    topic_count_df = pd.DataFrame(topic_counts)
    no_clusters = []
    outputs = []
    for i in range(clusters_df.shape[0]):
        similar_idx = [int(x) for x in clusters_df.iloc[i].tolist()[0].split(' ')]
        if (clusters_df.index[i]!=-1):
            mask = pd.DataFrame(np.zeros(topic_count_df.shape))
            mask[mask.columns[similar_idx]]=1
            masked_topic_count = topic_count_df * mask
            topic_idx = np.argmax(masked_topic_count.sum(axis=0))
            input = list(all_topics[similar_idx])
            output = all_topics[topic_idx]
            outputs.append({'inputs':input, 'output':output})
        else:
            no_clusters = all_topics[similar_idx]

    return outputs
    #[{inputs:output}]


@app.route('/getTopics', methods=['POST'])
def getTopics():
    ignoreList = getIgnoreList()
    data = request.get_json()
    print ('got request')
    '''
    data = {'blocks':[
        {'text':"Supervised learning is the machine learning task of learning a function that maps an input to an output based on example input-output pairs.[1] It infers a function      from labeled training data consisting of a set of      training examples.[2] In supervised learning, each      example is a pair consisting of an input object      (typically a vector) and a desired output value (also      called the supervisory signal). A supervised learning      algorithm analyzes the training data and produces an      inferred function, which can be used for mapping new      examples. An optimal scenario will allow for the algorithm      to correctly determine the class labels for unseen      instances. This requires the learning algorithm to      generalize from the training data to unseen situations      in a 'reasonable' way (see inductive bias)",
        "id":0
        }]}
    '''
    blocks = data['blocks']
    docs = [preprocess(block['text']) for block in blocks]
    #vectorizer = CountVectorizer(ngram_range=(3,3), stop_words="english").fit(docs)
    if (len(docs)==0):
        return []
    vectorizer.fit_transform(docs)
    #this gets keywords out of the whole document

    all_candidates = np.array([phrase for phrase in vectorizer.get_feature_names_out() if allOkayToAdd(phrase, ignoreList)])
    doc_embeddings = model.encode(docs)
    all_candidate_embeddings = model.encode(all_candidates)
    topics = []
    #need to do cosine similarity one block at a time
    for i, doc_embedding in enumerate(doc_embeddings):
        print(i)
        text = blocks[i]['text']
        candidates_idx = [i for i, candidate in enumerate(all_candidates) if candidate in text]
        candidates = all_candidates[candidates_idx]
        candidate_embeddings = all_candidate_embeddings[candidates_idx]

        if len(doc_embedding)>0 and len(candidate_embeddings)>0:
            #distances = cosine_similarity([doc_embedding], candidate_embeddings)
            keywords = mmr(doc_embedding, candidates, candidate_embeddings)
            if len(keywords)>0:
                high_scoring_keywords = [keyword for keyword in keywords if keyword[1]>0.2]

                if len(high_scoring_keywords)==0:
                    topics.append({'id':blocks[i]['id'], 'topics':[sorted(keywords, key=itemgetter(1), reverse=True)[0]]})
                else:
                    topics.append({'id':blocks[i]['id'], 'topics':high_scoring_keywords})
            '''
            #select all high scoring keywords
            keywords = [candidates[i] for i in distances.argsort()[0] if distances[0][i]>0.2]

            #select only the most relevant keyword if there isn't a high scoring keyword
            if len(keywords)==0:
                keywords = [candidates[i] for i in distances.argsort()[0][-1:] if distances[0][i]>0]

            allKeywords = [(candidates[i], distances[0][i]) for i in distances.argsort()[0]]
            '''


    combination_topics = combineSimilarTopics(docs, topics)
    new_topics = []
    for row in topics:
        outputs = []
        for topic in row['topics']:
            replaceWith = topic
            for _row in combination_topics:
                if topic[0] in _row['inputs']:
                    replaceWith = (_row['output'], replaceWith[1])
                    print (replaceWith)
                    break
            outputs.append(replaceWith)

        new_topics.append({'id':row['id'], 'topics':outputs})

    return new_topics
    #print ([keyword for keyword in keywords if keyword[1]>0])
    #print (standardiseTopics(topics))
    #return standardiseTopics(topics)

#getTopics()
