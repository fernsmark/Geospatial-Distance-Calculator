from flask import Flask, request, make_response
import os, time
from decimal import *
#from mysql.connector import (connection)
import MySQLdb
import memcache  
app = Flask(__name__)

@app.route('/')
def hello_world():
	return app.send_static_file('index.html')
 
		
@app.route('/search', methods=['POST','GET'])
def search():
	cnx =  MySQLdb.connect(host="testdb.cqfed169jgaj.us-west-2.rds.amazonaws.com", port=3306, user="root", passwd="mark12345", db="quiz")
	mc = memcache.Client(['cloudcomputing.itdhng.0001.usw2.cache.amazonaws.com:11211'], debug=0)
	cursor = cnx.cursor()
	#total_time = time.time()
	lon= lat =Decimal(0.0)
	k= "k"
	d= "d"
	if request.method=='POST':
		#return ('Welcome '+ request.form['username'])
		o_city= request.form['city']
		o_code= request.form['code']
		
		o_k= request.form['k']
		o_d= request.form['d']
		
		o_longitude= request.form['longitude']
		o_latitude= request.form['latitude']
		
		if o_k!="" and o_longitude=="" and o_latitude=="" :
			skip_k=0
			key= o_city + o_code + k
			print key
			o_k=int(o_k)
			o_k=o_k+1
			obj = mc.get(key.encode('utf-8'))
			
			o_d=50
			print 'k cities'
		elif o_d!="" and o_longitude=="" and o_latitude=="":
			skip_k=1
			key= o_city + o_code + d
			obj = mc.get(key.encode('utf-8'))
			print key
			o_d=int(o_d)
			o_k=100
			print 'd distance'
		elif o_longitude!="" and o_latitude!="" and o_k!="":
			skip_k=0
			print 'Long and Lat and k'
			key= o_longitude + o_latitude+o_k + k
			print key
			o_k=int(o_k)
			o_k=o_k+1
			obj = mc.get(key.encode('utf-8'))
			
			o_d=50
		else :	
			skip_k=1
			print 'Long and Lat and d'
			key= o_longitude + o_latitude+o_d + d
			print key
			o_d=int(o_d)
			obj = mc.get(key.encode('utf-8'))
			o_k=100
		
		total_time = time.time()
		if obj:
			i = 0
			#retreive from mem
			print 'Hi memcached found'
			list = "<tr><td>City</td> <td>State</td> <td>Longitude</td> <td>Latitude</td> <td>Distance</td></tr>"
			for row in obj :
				i +=1
				list= list+ "<tr><td>"+row[1]+"</td><td>"+str(row[2])+"</td><td>"+str(row[3])+"</td><td>"+str(row[4])+"</td><td>"+str(row[5])+"</td></tr>"
				if i==o_k and skip_k==0:
					break
				elif row[5]>=o_d:
					break
					
			#will be executed if there exists some data that is not in memcached when k is given		
			if i<o_k and skip_k==0:
				print "Not all in memcache"
				
				if o_code=='':
					query = ("SELECT lat, lon, city FROM city_info WHERE city= %s")
					cursor.execute(query, (o_city,))
				else :
					query = ("SELECT lat, lon, city FROM city_info WHERE city= %s and code= %s" )
					cursor.execute(query, (o_city,o_code,))
					
				for (lat, lon, city) in cursor:
					print("{}, {} are the lat and lon resp {}".format(lat, lon, city))
					
				lat=str(lat)
				lon=str(lon)
				o_d=str(o_d)
				o_k=str(o_k)
				
				query= ("SELECT country,city,code,lat,lon, 6371  * 2 * ASIN(SQRT(POWER(SIN(("+lat+"-abs(dest.lat)) * pi()/180 / 2), 2) "
						"+  COS("+lat+" * pi()/180 ) * COS(abs(dest.lat) * pi()/180)*  POWER(SIN(("+lon+" - dest.lon) * pi()/180 / 2), 2) )) as  distance "
						"FROM city_info dest "
						"where (dest.lon between ("+lon+"-"+o_d+"/abs(cos(radians("+lat+"))*69)) and ("+lon+"+"+o_d+"/abs(cos(radians("+lat+"))*69))) "
						"and (dest.lat between ("+lat+"-("+o_d+"/69)) and ("+lat+"+("+o_d+"/69))) "
						"having distance < "+o_d+" ORDER BY distance limit "+o_k)
										
				cursor.execute(query)
				print query
				data = cursor.fetchall()
				#del list[:] 
				###### lookup auto refresh page
				for row in data :
					list= list+ "<tr><td>"+row[1]+"</td><td>"+str(row[2])+"</td><td>"+str(row[3])+"</td><td>"+str(row[4])+"</td><td>"+str(row[5])+"</td></tr>"
			
				cursor.close()
				
				#obj= obj+ data
				#for row in obj :
				#	print row[1], row[2],row[3], row[4], row[5] 
				mc.set(key.encode('utf-8'), data)   #sets key for memcached	
				
				
				
			#will be executed if there exists some data that is not in memcached when d is given	
			if row[5]<o_d and skip_k==1:
				betw= row[5]
				print "Not all in memcache"
				
				if o_code=='':
					query = ("SELECT lat, lon, city FROM city_info WHERE city= %s")
					cursor.execute(query, (o_city,))
				else :
					query = ("SELECT lat, lon, city FROM city_info WHERE city= %s and code= %s" )
					cursor.execute(query, (o_city,o_code,))
					
				for (lat, lon, city) in cursor:
					print("{}, {} are the lat and lon resp {}".format(lat, lon, city))
					
				lat=str(lat)
				lon=str(lon)
				o_d=str(o_d+1)
				o_k=str(o_k)
				
				query= ("SELECT country,city,code,lat,lon, 6371  * 2 * ASIN(SQRT(POWER(SIN(("+lat+"-abs(dest.lat)) * pi()/180 / 2), 2) "
						"+  COS("+lat+" * pi()/180 ) * COS(abs(dest.lat) * pi()/180)*  POWER(SIN(("+lon+" - dest.lon) * pi()/180 / 2), 2) )) as  distance "
						"FROM city_info dest "
						"where (dest.lon between ("+lon+"-"+o_d+"/abs(cos(radians("+lat+"))*69)) and ("+lon+"+"+o_d+"/abs(cos(radians("+lat+"))*69))) "
						"and (dest.lat between ("+lat+"-("+o_d+"/69)) and ("+lat+"+("+o_d+"/69))) "
						"having distance between "+str(betw)+" and "+o_d+" ORDER BY distance ")
										
				cursor.execute(query)
				print query
				data = cursor.fetchall()
				for row in data :
					list= list+ "<tr><td>"+row[1]+"</td><td>"+str(row[2])+"</td><td>"+str(row[3])+"</td><td>"+str(row[4])+"</td><td>"+str(row[5])+"</td></tr>"
			
				cursor.close()
				
				obj= obj+ data
				for row in obj :
					print row[1], row[2],row[3], row[4], row[5] 
				mc.set(key.encode('utf-8'), obj)   #sets key for memcached	
				
				
				
			print i
			total_time = Decimal(time.time()-total_time)
			
			
		else:
			if o_longitude=="" and o_latitude=="":
				if o_code=='':
					query = ("SELECT lat, lon, city FROM city_info WHERE city= %s")
					cursor.execute(query, (o_city,))
				else :
					query = ("SELECT lat, lon, city FROM city_info WHERE city= %s and code= %s" )
					cursor.execute(query, (o_city,o_code,))
					
				for (lat, lon, city) in cursor:
					print("{}, {} are the lat and lon resp {}".format(lat, lon, city))	
					
			else :
				lat= o_latitude
				lon= o_longitude
							
			lat=str(lat)
			lon=str(lon)
			o_d=str(o_d)
			o_k=str(o_k)
			
			condition =True
			while condition:
				city_counter=0
				# km- 6371 , miles- 3956
				query= ("SELECT country,city,code,lat,lon, 6371  * 2 * ASIN(SQRT(POWER(SIN(("+lat+"-abs(dest.lat)) * pi()/180 / 2), 2) "
						"+  COS("+lat+" * pi()/180 ) * COS(abs(dest.lat) * pi()/180)*  POWER(SIN(("+lon+" - dest.lon) * pi()/180 / 2), 2) )) as  distance "
						"FROM city_info dest "
						"where (dest.lon between ("+lon+"-"+o_d+"/abs(cos(radians("+lat+"))*69)) and ("+lon+"+"+o_d+"/abs(cos(radians("+lat+"))*69))) "
						"and (dest.lat between ("+lat+"-("+o_d+"/69)) and ("+lat+"+("+o_d+"/69))) "
						"having distance < "+o_d+" ORDER BY distance limit "+o_k)
										
				cursor.execute(query)
				
				if skip_k!=1:
					for row in cursor:
						city_counter=city_counter+1
					
					print "city counter is ",city_counter
					if city_counter>=int(o_k):
						print "inside"
						condition= False
						break
					o_d=int(o_d)	
					o_d= o_d+500
					o_d=str(o_d)
					print o_d
				else:
					condition= False
			
			#for (lat, lon, city) in cursor:
			#	print("{}, {} , {}".format(lat, lon, city))
			
			print query
						
			data = cursor.fetchall()
			mc.set(key.encode('utf-8'), data)   #sets key for memcached
			list = "<tr><td>City</td> <td>State</td> <td>Longitude</td> <td>Latitude</td> <td>Distance</td></tr>"
			i = 0
			for row in data :
				i += 1
				#print row[1], row[2],row[3], row[4], row[5]   -- uncomment to view the result in terminal
				#list= list +" "+row[1]+" "+str(row[2])+" "+str(row[3])+" "+str(row[4])+" "+str(row[5])+"<br>"
				list= list+ "<tr><td>"+row[1]+"</td><td>"+str(row[2])+"</td><td>"+str(row[3])+"</td><td>"+str(row[4])+"</td><td>"+str(row[5])+"</td></tr>"
			print i	
			cursor.close()
			#cnx.close()
			total_time = Decimal(time.time()-total_time)
			print 'from db'
			
		return '''<!DOCTYPE html>
				<html>

				  <head>
					<title>Python Flask Application</title>
					<meta charset="utf-8">
					<meta http-equiv="X-UA-Compatible" content="IE=edge">
					<meta name="viewport" content="width=device-width, initial-scale=1">
					<link rel="stylesheet" href="static/stylesheets/style.css">
				  </head>

				  <body><table>''' + list + '''</table><br>
				  '''+ str(total_time) +'''
				  </body>
				  
				</html>'''
	
		#return app.send_static_file('upload.html')
	else:
		return 'Not allowed'
		
	
if __name__ == '__main__':
	app.run(debug=True)
	#app.run(host='0.0.0.0', port=int(port), debug=True)	