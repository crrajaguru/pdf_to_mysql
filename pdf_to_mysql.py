import tabula
import mysql.connector
from datetime import datetime, date

# pdf file url
pdf_url = "https://mausam.imd.gov.in/chennai/mcdata/dailyweekly.pdf"

# mysql connection
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="*********",
  database="weather"
)
mycursor = mydb.cursor()
# call tabula read_pdf function from url
df = tabula.read_pdf(pdf_url, stream=True, output_format="json", pages="all")
# Mysql Insert Query 
sql = "INSERT INTO table_rainfall (dist_name,today_actual,today_normal,today_pdn,monsoon_actual,monsoon_normal,monsoon_pdn,updated_date) values (%s,%s,%s,%s,%s,%s,%s,%s)"
val = []
global mydate
for data in df:
    for row in data['data']:
        if len(row) > 1:
            myrow = row[1]['text']
        else:
            myrow = row[0]['text']
        
        mylist = myrow.split(' ')       
        if 6 < len(mylist) < 10 and ('MINISTRY' not in mylist):
            if 'NE-MONSOON' in mylist:
                mydate = datetime.strptime(mylist[mylist.index('From') - 1], '%d.%m.%Y').date()


            if mylist[0] != 'SUBDIVISION' and mylist[0] != 'SW-MONSOON' and mylist[0] != 'SW' and mylist[0] != 'NORMAL' and mylist[1] != 'PUDUCHERRY' and mylist[1] != 'KARAIKAL' and mylist[1] != 'RAINFALL':
                
                if mylist[1] == 'TAMIL':               
                    mylist.remove('TAMIL')
                    mylist.remove('NADU')
                    mylist[0] = 'TAMIL NADU'
                    mylist.append(datetime.strftime(mydate, '%Y-%m-%d'))
                    val.append(tuple(mylist))                    
                elif mylist[0] != 'GOVERNMENT':                  
                    mylist.pop(0)
                    mylist.append(datetime.strftime(mydate, '%Y-%m-%d'))
                    val.append(tuple(mylist))       


mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount, "rows was inserted." )
