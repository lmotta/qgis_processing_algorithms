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
- None

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
import urllib.request, urllib.error
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
        
    def _progress(self, name):
        self.c_url += 1
        percent = int( self.c_url * self.f_perc_url )
        msg = f"Download {name} ({self.c_url} of {self.t_url})..."
        self.feedback.setProgress( percent )
        self.feedback.setProgressText( msg )
        
    def readUrls(self):
        with open( self.pathfile, 'r') as content_file:
            content = content_file.read()    

        urls = content.split('\n')
        if urls[-1] == '': del urls[-1]

        return urls
        
    def downloadsImages(self, urls):
        def download(url):
            # url: http://www2.dgi.inpe.br/api/download/TIFF/CBERS4A/2020_09/CBERS_4A_WPM_RAW_2020_09_16.14_13_00_ETC2/218_128_0/4_BC_UTM_WGS84/CBERS_4A_WPM_20200916_218_128_L4_BAND0.tif?key=motta.luiz@gmail.com&collection=CBERS4A_WPM_L4_DN&scene_id=CBERS4A_WPM21812820200916
            ds = None
            name = f"{url.split('?')[0].split('/')[-1]}"
            image = f"{self.pathdir}{os.path.sep}{name}"
            image_download = f"{image}.download"
            try:
                if not os.path.exists( image ):
                    _response = urllib.request.urlopen(url, timeout=5) # Check if can access
                    urllib.request.urlretrieve( url, image_download )
                    shutil.move( image_download, image )
                if image.split('.')[-1] == 'tif':
                    ds = gdal.Open( image, GA_ReadOnly )
            except urllib.error.URLError as e: # urllib
                msg = f"Url '{url}': n{e.reason}"
                self.feedback.reportError( msg )
                return False
            except RuntimeError: # gdal
                os.remove( image )
                msg = f"Url '{url}': Error open image"
                self.feedback.reportError( msg )
                return False

            self._progress( name )
            ds = None
            return True
            
        self.t_url = len( urls )
        self.f_perc_url = 100.0 / self.t_url
        
        pool = ThreadPool(processes=4)
        mapResult = pool.map_async( download, urls )
        results = [ r for r in mapResult.get() ]
        pool.close()
        
        return not False in results

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

    isOk = dcw.downloadsImages( urls )
    msg = f"Finished all {dcw.t_url} files in '{input_pathdir}'" if isOk else 'Error download files'
    return { 'MESSAGE': msg }
