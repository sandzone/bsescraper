from flask import Flask, request, stream_with_context
from keyphrase_vectorizers import KeyphraseCountVectorizer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise  import cosine_similarity
from nltk import pos_tag
from nltk.stem.porter import PorterStemmer
from nltk.util import ngrams
from nltk import word_tokenize
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

    ]

    allTopics = [topic for row in topics for topic in row['topics']]
    outputs = []
    for topic in allTopics:
        tokens = [stemmer.stem(token) for token in word_tokenize(topic)]
        output = {'input':topic, 'output':topic}
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

    if len(pattern.findall(phrase))>0 return False

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

@app.route('/getTopics', methods=['POST'])
def getTopics():
    ignoreList = getIgnoreList()
    #data = request.get_json()

    data = {'blocks':[
        {'text':"“ABB India Limited Q3 CY2018 Earnings Call”",
        "id":0
        }]}

    blocks = data['blocks']
    docs = [preprocess(block['text']) for block in blocks]
    vectorizer.fit_transform(docs)
    #this gets keywords out of the whole document
    candidates = [phrase for phrase in vectorizer.get_feature_names_out() if allOkayToAdd(phrase, ignoreList)]
    doc_embeddings = model.encode(docs)
    candidate_embeddings = model.encode(candidates)
    topics = []
    #need to do cosine similarity one block at a time
    print (len(doc_embeddings))
    for i, doc_embedding in enumerate(doc_embeddings):
        print(i)
        text = blocks[i]['text']
        candidate_indices = [i for i, candidate in enumerate(candidates) if candidate in text]
        relevant_candidates = [candidates[i] for i in candidate_indices]
        relevant_candidate_embeddings = [candidate_embeddings[i] for i in candidate_indices]
        if len(doc_embedding)>0 and len(relevant_candidate_embeddings)>0:
            distances = cosine_similarity([doc_embedding], relevant_candidate_embeddings)
            keywords = [relevant_candidates[i] for i in distances.argsort()[0] if distances[0][i]>0.2]

            if len(keywords)==0:
                keywords = [relevant_candidates[i] for i in distances.argsort()[0][-1:] if distances[0][i]>0]

            allKeywords = [(relevant_candidates[i], distances[0][i]) for i in distances.argsort()[0]]

            if len(keywords)>0:
                topics.append({'id':blocks[i]['id'], 'topics':keywords})

    print (standardiseTopics(topics))
    return standardiseTopics(topics)

getTopics()
