# -------------------------------------------------------------------------
# AUTHOR: Rida Siddiqui
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 hrs
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import string

def connectDataBase():
    # Create a database connection object using pymongo
    # --> add your Python code here

    DB_HOST = 'localhost:27017'

    try:
        client = MongoClient(host=[DB_HOST])
        db = client.corpus
        return db

    except:
        print("Database not connected")


def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here

    termsInDoc = docText.lower().translate(str.maketrans('', '', string.punctuation))
    termsInDocList = termsInDoc.split(' ')
    print(termsInDocList)
    unique_terms = list(set(termsInDocList))

    termsList = []
    for x in unique_terms:
        termsList.append({"term": x, "count": termsInDocList.count(x)})

    col.insert_one(
        {"_id": docId, "text": docText, "title": docTitle, "date": docDate, "category": docCat, "terms": termsList})


def deleteDocument(col, docId):
    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})



def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)


def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = col.aggregate([{"$unwind": "$terms"}, {"$group": {"_id": {"terms": "$terms", "title": "$title"}}}, {
        "$project": {"_id": 0, "title": "$_id.title", "term": "$_id.terms.term", "count": "$_id.terms.count"}}])

    for x in index:
        print(x)