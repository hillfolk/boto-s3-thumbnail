#!/usr/bin/python
# -*- coding: utf-8 -*-

import boto
import boto.ec2
import MySQLdb as mdb
import sys
import os
import itertools
import PIL
from PIL import Image



def generate_dicts(cur):
    import itertools
    fieldnames = [d[0].lower() for d in cur.description ]
    while True:
        rows = cur.fetchmany()
        if not rows:return
        for row in rows:
            yield dict(itertools.izip(fieldnames,row))


def connect_rds():
    con = mdb.connect('woodongpandb.cowkse8uushl.ap-northeast-1.rds.amazonaws.com', 'markadmin', 'mark130620', 'woodongpan')
    return con

def getFileList( rds_conn ):
    filelist = list()
    cur = rds_conn.cursor()
    cur.execute("select ATCH_FILE_ID, FILE_SN, FILE_STRE_COURS, STRE_FILE_NM, FILE_URL,FILE_EXTSN from COMTNFILEDETAIL")
    for r in generate_dicts(cur):
        filelist.append(r)
    cur.close()
    return filelist

def updateFileList(con,file):
    cur = con.cursor()
    cur.execute("update COMTNFILEDETAIL set THUMBNAIL_URL = %s where STRE_FILE_NM = %s ",(file['thumb_url'],file['stre_file_nm']))
    if file['file_sn'] == 0:
        cur.execute("update WDMMENU set MENUTHUMBNAILURL = %s where MENUIMGFILE = %s ",(file['thumb_url'],file['atch_file_id']))
    con.commit()

    return True

def createThumbnail(filename,width,folder):
    
    try:
        img = Image.open(filename)
        if img.size[0] > width:
            wpercent = (width/float(img.size[0]))
            hsize = int((float(img.size[1])*float(wpercent)))
            img = img.resize((width,hsize), PIL.Image.ANTIALIAS)
            outfile = folder+'/'+filename
            img.save(outfile)
            print 'Thumbnail 을 생성했습니다.'
            return outfile
        else:
            outfile = folder+'/'+filename
            img.save(outfile)
            return outfile
    except Exception, e:
        
        return False
   
    
    

rds = connect_rds()
filelist = getFileList(rds)

print filelist

boto.set_stream_logger('boto')
bucket_name = 'akiai7tf5uii75j7hj2a-wdm-markmedia'
s3 = boto.connect_s3('AKIAI7TF5UII75J7HJ2A','9jMbpCUNtdzcbVYKINg+6BbCsfw4VKwhOelKw1Ot');
bucket = s3.get_bucket(bucket_name)

for f in filelist:
    print f
    key = bucket.lookup(f['file_stre_cours']+f['stre_file_nm'])
    print f['stre_file_nm']
    if f['file_extsn'].lower() in ('jpg','png','jpeg'):
      if key == None :
          print '해당 파일이 없습니다.'
      else:
          filename = f['stre_file_nm']+'.'+f['file_extsn'];
          key.get_contents_to_filename(filename)
          outfile = createThumbnail(filename,320,'thumbnail320')
          if outfile != False:
              newkey = bucket.new_key(f['file_stre_cours']+'thumbnail320/'+f['stre_file_nm'])
              newkey.set_contents_from_filename(outfile)
              newkey.set_acl('public-read')
              url = newkey.generate_url(0, query_auth=False, force_http=True,policy='public-read')
	      print url
              f['thumb_url'] = url
              updateFileList(rds,f)
	      os.remove(outfile)
          os.remove(filename)


          
          
