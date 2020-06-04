import csv
import pandas as pd
from flask import Flask, request, jsonify
from io import StringIO
from werkzeug.wrappers import Response

app = Flask(__name__)
globalDf = pd.DataFrame()
def fileToDataFrame(filePath):
    try:
        df = pd.read_csv(filePath)
        global globalDf
        globalDf = df
        return df
    except:
        try:
            df = pd.read_excel(filePath)
            globalDf = df
            return df
        except:
            raise FileNotFoundError("Not able to convert file to dataframe")

def serializedUniqueValues(listOfValues):
    for index in range(len(listOfValues)):
        converted_value = getattr(listOfValues[index], "tolist", lambda: listOfValues[index])()
        listOfValues[index] = converted_value
    return listOfValues

@app.route('/dropDownFilter', methods=["POST","GET","DELETE","PUT"])
def getDropDownFilter():
    if (request.method == 'GET'):
        filePath = request.args.get('filePath')
        try:
            fileDataFrame = fileToDataFrame(filePath)
            uniqueValueForAttributes = {}
            for attributes in fileDataFrame:
                listOfUniqueValuesSerializable = serializedUniqueValues(list(fileDataFrame[attributes].unique()))
                uniqueValueForAttributes[attributes] = listOfUniqueValuesSerializable
            return jsonify(uniqueValueForAttributes),200
        except FileNotFoundError:
            return jsonify({"reason": "file path invalid or file cannot be read as csv or xlsx"}),400
        except Exception as e:
            print(e)
            return jsonify({"reason": "internal server error"}),500
    else:
        return jsonify({"reason": "method not allowed"}),405




def getRowsAsListOfTuplesFromDataFrame(dataFrame):
    return [tuple(x) for x in dataFrame.to_numpy()]

@app.route('/downloadFilteredExcel', methods=["GET"])
def downloadFilteredExcelAsCsv():
    dataFrameToDownload = globalDf
    tupleRows = getRowsAsListOfTuplesFromDataFrame(dataFrameToDownload)
    def generate():
        data = StringIO()
        writer = csv.writer(data)

        # write header
        writer.writerow(tuple(dataFrameToDownload.columns.values))
        yield data.getvalue()
        data.seek(0)
        data.truncate(0)

        # write each log item
        for item in tupleRows:
            writer.writerow(item)
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
    response = Response(generate(), mimetype='text/csv')
    response.headers.set("Content-Disposition", "attachment", filename="log.csv")
    return response

if __name__ == '__main__':
    app.run(debug=True, port=5000)

