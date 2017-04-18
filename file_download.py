import urllib2
import time
import os

os.system('clear')

for year in range(2001,2008):
	for month in range(1,13):
		for day in range(1,32):
			try:
				time.sleep(3)
				url = "https://www.forexite.com/free_forex_quotes/"+str(year)+"/"+str(month).zfill(2)+"/"+str(day).zfill(2)+str(month).zfill(2)+str(year-2000).zfill(2)+".zip"
				print url
				file_name = str(year)+"/"+url.split('/')[-1]
				if not os.path.exists(file_name):
					u = urllib2.urlopen(url)
					f = open(file_name, 'wb')
					meta = u.info()
					file_size = int(meta.getheaders("Content-Length")[0])
					print "Downloading: %s Bytes: %s" % (file_name, file_size)

					file_size_dl = 0
					block_sz = 8192
					while True:
						buffer = u.read(block_sz)
						if not buffer:
							break

						file_size_dl += len(buffer)
						f.write(buffer)
						status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
						status = status + chr(8)*(len(status)+1)
						print status,

					f.close()
			except Exception as e:
				print str(e)