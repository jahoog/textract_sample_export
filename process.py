import boto3
import os
from trp import Document
import trp.trp2 as t2

client = boto3.client('textract')

folderName = "docs"
dir_list = os.listdir(folderName)


def processFile(fileName):

    documentName = "docs\\" + fileName
    print("Processing File: ", fileName)
    outputDirectoryName = os.path.splitext(documentName)[0]
    os.makedirs(outputDirectoryName, exist_ok=True)

    # Call Amazon Textract
    document = open(documentName, "rb")

    response = client.analyze_document(
        Document={
            'Bytes': document.read()
        },
        FeatureTypes=['TABLES', 'FORMS', 'QUERIES'],
        QueriesConfig={
            "Queries": [{
                "Text": "NAME",
                "Alias": "PERSON_NAME"
            },
                {
                "Text": "Phone",
                        "Alias": "PHONE_NUMBER"
            }]
        }
    )

    doc = Document(response)

    # write tables
    pageOutput = open(outputDirectoryName + "\\tables.csv", "w")

    for page in doc.pages:
        # Print tables
        for table in page.tables:
            for r, row in enumerate(table.rows):
                cellCount = 0
                for c, cell in enumerate(row.cells):
                    if (cellCount > 0):
                        pageOutput.write(",")
                    pageOutput.write(cell.text)
                    cellCount = cellCount + 1
                pageOutput.write("\n")

    # write form fields
    fieldOutput = open(outputDirectoryName + "\\forms.csv", "w")
    fieldOutput.write("field, value\n")

    for page in doc.pages:
        # Print fields
        for field in page.form.fields:
            fieldOutput.write("{},{}\n".format(field.key, field.value))

    # write query fields
    queryOutput = open(outputDirectoryName + "\\queries.csv", "w")
    queryOutput.write("query, alias, answer\n")

    queryDoc = t2.TDocumentSchema().load(response)

    for page in queryDoc.pages:
        query_answers = queryDoc.get_query_answers(page=page)
        for query in query_answers:
            queryOutput.write("{},{},{}\n".format(
                query[0], query[1], query[2]))


for fName in dir_list:
    try:
        processFile(fName)
    except:
        print("Error processing file: ", fName)
