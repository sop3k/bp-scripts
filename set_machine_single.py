import sqlite3, multiprocessing
import glob, os, fnmatch, sys, datetime, time

def find_files(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename

def get_exporter_hits(exporter_db, project_name):
	conn = sqlite3.connect(exporter_db)
	cursor = conn.cursor()
	query = cursor.execute("select ip, port, date, time, guid, hash, filename from hits_%s" % (project_name,))
	return query.fetchall()
	
def process_db(hits, hits_db):
	"""
	"""	
	hits_c = sqlite3.connect(hits_db)
	for ip, port, date, time, guid, hash, filename in hits:	
		cursor = hits_c.cursor()
		d = datetime.datetime.strptime(date.rstrip('Z'), '%Y-%m-%d %H:%M:%S')
		#and year=? and month=? and day=? and hour=? and minute=? and second=?
		#d.year, d.month, d.day, d.hour, d.minute, d.second,
		stm = "SELECT * from hits where hash=? and ip=? and port=? and guid=?"
		query = cursor.execute(stm, (hash, ip, port, guid))
		yield ((ip, port, date, time, guid, hash, filename), query.fetchall())
	
def process_all(exporter_db, db_dir, project):
	"""
	"""
	hits = get_exporter_hits(exporter_db, project)
	print "Found ", len(hits), "hits in ", exporter_db
	for db in find_files(db_dir, "*.db"):
		t0 = time.clock()
		print "Start processing ", db, "...", 
		for x, matched in process_db(hits, db):
			if len(matched) > 0:
				print x, "---->", db	
		print str(datetime.timedelta(seconds=time.clock() - t0))
		
if __name__ == "__main__":
	process_all(sys.argv[1], sys.argv[2], sys.argv[3])
