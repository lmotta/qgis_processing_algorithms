#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
/***************************************************************************
Name                 : Algorithm for QGIS - download_inpe_cbers4a_wpm
Description          : Download CBERS WPM files from INPE
Date                 : October, 2020
copyright            : (C) 2020 by Luiz Motta
email                : motta.luiz@gmail.com

Dependences:
- gdal
- mod_py

Updates:
- 2020-10-05:
Added Canceled

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""
__author__ = 'Luiz Motta'
__date__ = '2020-10-04'
__copyright__ = '(C) 2020, Luiz Motta'
__revision__ = '$Format:%H$'
 
import sys, os, shutil

import contextlib
from urllib.request import urlopen 
from urllib.error import URLError

from multiprocessing.pool import ThreadPool

from osgeo import gdal
from osgeo.gdalconst import GA_ReadOnly
gdal.UseExceptions()

from qgis import processing
from qgis.processing import alg


class FilePathType(object):
    def __init__(self):
        pass
    def __call__(self, value):
        msg = None
        if not os.path.isfile( value ):
            msg = f"Missing file '{value}'"
        if not msg is None:
            raise argparse.ArgumentTypeError( msg )
        return value
 

class DownloadCbersWpm():
    def __init__(self, pathfile, pathdir, feedback):
        self.pathfile, self.pathdir = pathfile, pathdir
        self.feedback = feedback
        self.c_url, self.t_url, self.f_perc_url = 0, 0, 0
        self.isOkDownload = True
       
    def readUrls(self):
        with open( self.pathfile, 'r') as content_file:
            content = content_file.read()    

        urls = content.split('\n')
        if urls[-1] == '': del urls[-1]

        return urls
        
    def downloadsImages(self, urls):
        def download(url):
            def urlretrieve(url, filename, isCanceled):
                """
                Adaptation from '/usr/lib/python3.8/urllib/request.py'
                """
                try:
                    response = urlopen( url )
                except URLError as e: # urllib
                    msg = f"Url '{url}': n{e.reason}"
                    return { 'isOk': False, 'message': msg}

                size = -1
                read = 0
                bs = 1024*8
                with contextlib.closing( response ) as fp:
                    headers = fp.info()
                    if "content-length" in headers:
                        size = int(headers["Content-Length"])

                    tfp = open(filename, 'wb')
                    with tfp:
                        while True:
                            if isCanceled():
                                return { 'isOk': False, 'message': 'Canceled' }
                            block = fp.read( bs )
                            if not block:
                                break
                            read += len(block)
                            tfp.write( block )

                if size >= 0 and read < size:
                    msg = f"Retrieval incomplete: got only {read} out of {size} bytes"
                    return { 'isOk': False, 'message': msg }
                
                return { 'isOk': True }
            
            def progress(name):
                self.c_url += 1
                percent = int( self.c_url * self.f_perc_url )
                msg = f"Download {name} ({self.c_url} of {self.t_url})..."
                self.feedback.setProgress( percent )
                self.feedback.setProgressText( msg )

            ds = None
            name = f"{url.split('?')[0].split('/')[-1]}"
            image = f"{self.pathdir}{os.path.sep}{name}"
            image_download = f"{image}.download"
            if not os.path.exists( image ):
                r = urlretrieve( url, image_download, self.feedback.isCanceled )
                if not r['isOk']:
                    msg = f"{r['message']}. {name}"
                    self.feedback.reportError( msg )
                    self.isOkDownload = False
                    if os.path.isfile( image_download ):
                        os.remove( image_download )
                    return

                shutil.move( image_download, image )
                if image.split('.')[-1] == 'tif':
                    try:
                        ds = gdal.Open( image, GA_ReadOnly )
                    except RuntimeError: # gdal
                        os.remove( image )
                        msg = f"Url for '{name}': Error open image"
                        self.feedback.reportError( msg )
                        self.isOkDownload = False
                        return
                    ds = None
                    
            progress( name )

        self.t_url = len( urls )
        self.f_perc_url = 100.0 / self.t_url
        
        pool = ThreadPool( processes=4 )
        mapResult = pool.map_async( download, urls )
        [ r for r in mapResult.get() ]
        pool.close()

@alg(name='cbers4downloadwpmalg', label='Download CBERS WPM files from INPE',
     group='lmottacripts', group_label='Lmotta scripts')

@alg.input(type=alg.FILE, name='FILE_INPE', extension='txt',
           label='Input file Inpe(inpe_catalog_Year_Month_Day_Hour_Minute_Second.txt)')

@alg.input(type=alg.FOLDER_DEST, name='FOLDER_IMAGES', label='Folder of images downloads')

@alg.output(type=alg.STRING, name='MESSAGE', label='Message of processing')

def cbers4downloadwpmalg(instance, parameters, context, feedback, inputs):
    """
    Download CBERS WPM files from INPE.
    """
    input_pathfile = instance.parameterAsFile(parameters, 'FILE_INPE', context )
    input_pathdir = instance.parameterAsFile(parameters, 'FOLDER_IMAGES', context )

    dcw = DownloadCbersWpm( input_pathfile, input_pathdir, feedback )
    urls = dcw.readUrls()
    if len( urls ) == 0:
        msg = "Missing files in {pathfile}"
        self.feedback.reportError( msg )
        return { 'MESSAGE': msg }

    dcw.downloadsImages( urls )
    msg = f"Finished all {dcw.t_url} files in '{input_pathdir}'" if dcw.isOkDownload else 'Error download files'
    
    return { 'MESSAGE': msg }
