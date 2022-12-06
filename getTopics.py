from flask import Flask, request, stream_with_context
#from keyphrase_vectorizers import KeyphraseCountVectorizer

from keybert import KeyBERT
app = Flask(__name__)

@app.route('/getTopics', methods=['GET'])
def getTopics():
    doc = """Sanjeev Sharma: Well, ABB India is one of the strongest units from 360-degree point of view when it comes to   digital. I think that is perhaps the uniqueness of ABB India in the market, wherein our dependency outside India is pretty low. So, it is all about competence which we have developed over a period of time. When our customers are dealing with us they are dealing with us. So, all the revenue lines that run, typically they run through our books, barring maybe some of the software or maybe some licenses that we may have to import in. Barring that everything gets executed and booked here in India."""
    kw_model = KeyBERT()
    keywords = kw_model.extract_keywords(doc)

    #vectorizer = KeyphraseCountVectorizer()
    #document_keyphrase_matrix = vectorizer.fit_transform(docs).toarray()

    #print(keywords)
    #print(request.method)



    return 'done'

getTopics()
