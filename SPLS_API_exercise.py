from flask import request
from flask import jsonify, make_response
from flask import Flask
from shapely.geometry import Polygon, Point
from pyspark import SparkContext, SQLContext


# =====================================================

def estimate_price(house_feat):
    price = 0.0
    price += 75.0 * house_feat['GrLivArea']
    price += 54.0 * house_feat['TotalBsmtSF']
    price += 51.0 * house_feat['GarageArea']
    price += 671.0 * (house_feat['YearBuilt'] - 1990)

    return price


# =====================================================

app = Flask(__name__)


@app.route('/houseprice', methods=["GET"])
def elements():
    house_data = {}
    try:
        house_data['GrLivArea'] = int(request.args.get('GrLivArea'))
        house_data['TotalBsmtSF'] = int(request.args.get('TotalBsmtSF'))
        house_data['GarageArea'] = int(request.args.get('GarageArea'))
        house_data['YearBuilt'] = int(request.args.get('YearBuilt'))
    except Exception as e:
        http_response = make_response(
            jsonify({'Exception': 'All paramters must be numbers', 'ExceptionCause': e.args[0], 'ErrorCode': 500}))
        return http_response

    http_response = make_response(jsonify(
        {'HouseFeatures': house_data, 'PriceEstimate': estimate_price(house_data)}
    ))

    return http_response


@app.route('/houselookup', methods=["GET"])
def getAllHousesUnderPolygon():

    coordinateslist = []
    i = 1
    l = int(len(request.args)/2)
    outputlist = []

    for coord in range(int(len(request.args)/2)):
        xc = "x" + str(i)
        yc = "y" + str(i)
        x = request.args.get(xc)
        y = request.args.get(yc)
        p = Point(float(x), float(y))
        coordinateslist.append(p)
        i += 1

    poly = Polygon([[p.x, p.y] for p in coordinateslist])

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    linedf = sqlContext.read.option("header","false").format("csv").load("/home/oza/Downloads/house_coordinates.csv")
    rowdf = linedf.select("*").collect()
    linedf.show()

    for column in rowdf:
        x1 = column[1]
        y1 = column[2]
        x2 = column[3]
        y2 = column[4]
        x3 = column[5]
        y3 = column[6]
        x4 = column[7]
        y4 = column[8]

        p1 = Point(float(x1), float(y1))
        p2 = Point(float(x2), float(y2))
        p3 = Point(float(x3), float(y3))
        p4 = Point(float(x4), float(y4))



        # We will check whether a given point is within polygon coordinates,
        # If yes than we will add the house id inside the list

        if (isPointWithinPolygon(poly, p1, p2, p3, p4) == True):
            outputlist.insert(column[0])

    http_response = make_response(jsonify(outputlist))
    return http_response

def isPointWithinPolygon(polygon, point1, point2, point3, point4):
    if(point1.within(polygon) == True):
        if(point2.within(polygon) == True):
            if (point3.within(polygon) == True):
                if (point4.within(polygon) == True):
                    return True


# =====================================================

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
