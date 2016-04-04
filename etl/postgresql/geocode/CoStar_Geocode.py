__author__ = 'esa'
import urllib, json
from geopy.exc import GeocoderTimedOut
import csv
import timeit
import pandana


start = timeit.timeit()

##info = json.loads(data).get("results")[0].get("geometry").get("location") ##short
addresses = "T:\socioec\pecas\data\Floorspace\costar\Costar2016\Geocode\costar_geocode.csv"
output = "T:\socioec\pecas\data\Floorspace\costar\Costar2016\Geocode\costar_geocode_results.csv"

with open(output, 'wb') as csv_output:
    with open(addresses, 'rb') as csv_input:
        csv_writer = csv.writer(csv_output, quotechar='"')
        row_reader = csv.reader(csv_input, delimiter=',')

        csv_writer.writerow(['property_id','address','latitude','longitude'])

        for row in row_reader:
            address_row = row[1] + ", "+ row[4] + ", CA, "+ row[5]          ##Street Address, City, "CA", Zip   ##columns start at 0
            print row[1] + ", "+ row[4] + ", CA, "+ row[5]                  ##print to verify address and column order
            try:
                url = "http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false" %    (urllib.quote(address_row.replace(' ', '+')))
                google_response = urllib.urlopen(url)
                json_response = json.loads(google_response.read())
            except GeocoderTimedOut as e:
                print ("Error: geocode failed on input %s with message %s"%(address_fields, e.message))
            if json_response['results']:
                json_location = json_response['results'][0]
                address = json_response['results'][0]['formatted_address']
                latitude, longitude = json_location ['geometry']['location']['lat'], json_location['geometry']['location']['lng']
                print row[0], address, latitude, longitude
                csv_writer.writerow([row[0], address.encode('UTF-8'), latitude, longitude])

end = timeit.timeit()
print end - start
