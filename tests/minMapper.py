# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import glob
import os
import re
from lsst.daf.persistence import Mapper, ButlerLocation
import lsst.afw.image as afwImage

class FsRegistry(object):
    def __init__(self, location, pathTemplate):
        fmt = re.compile(r'%\((\w+)\).*?([diouxXeEfFgGcrs])')
        globString = fmt.sub('*', pathTemplate)
        last = 0
        self.fieldList = []
        intFields = []
        reString = ""
        n = 0
        for m in fmt.finditer(pathTemplate):
            fieldName = m.group(1)
            if fieldName in self.fieldList:
                fieldName += "_%d" % (n,)
                n += 1
            self.fieldList.append(fieldName)

            if m.group(2) not in 'crs':
                intFields.append(fieldName)
        
            prefix = pathTemplate[last:m.start(0)]
            last = m.end(0)
            reString += prefix

            if m.group(2) in 'crs':
                reString += r'(?P<' + fieldName + '>.+?)'
            elif m.group(2) in 'xX':
                reString += r'(?P<' + fieldName + '>[\dA-Fa-f]+?)'
            else:
                reString += r'(?P<' + fieldName + '>[\d.eE+-]+?)'

        reString += pathTemplate[last:] 

        curdir = os.getcwd()
        os.chdir(location)
        pathList = glob.glob(globString)
        os.chdir(curdir)
        self.tuples = []
        for path in pathList:
            dataId = re.search(reString, path).groupdict()
            idList = []
            for f in self.fieldList:
                if f in intFields:
                    idList.append(int(dataId[f]))
                else:
                    idList.append(dataId[f])
            self.tuples.append(tuple(idList))

    def getFields(self):
        return self.fieldList

class MinMapper(Mapper):
    def __init__(self, policy=None, root=".", calibRoot=None):
        Mapper.__init__(self)
        self.rawTemplate = "imsim_%(visit)d_%(raft)s_%(sensor)s_%(channel)s_E%(exposure)03d.fits"
        self.biasTemplate = "bias/imsim_0_%(raft)s_%(sensor)s_%(channel)s_E%(exposure)03d.fits.gz"
        self.darkTemplate = "dark/imsim_1_%(raft)s_%(sensor)s_%(channel)s_E%(exposure)03d.fits.gz"
        self.flatTemplate = "flat_r/imsim_2_%(raft)s_%(sensor)s_%(channel)s_E%(exposure)03d.fits.gz"
        self.fringeTemplate = "flat_r/imsim_2_%(raft)s_%(sensor)s_%(channel)s_E%(exposure)03d.fits.gz"
        self.root = root
        if policy.exists('root'):
            self.root = policy.getString('root')
        self.calibRoot = calibRoot
        if policy.exists('calibRoot'):
            self.root = policy.getString('calibRoot')
        if self.calibRoot is None:
            self.calibRoot = self.root
        self.rawRegistry = FsRegistry(self.root, self.rawTemplate)
        self.keys = self.rawRegistry.getFields()

    def getKeys(self):
        return self.keys

    def _pathMapper(self, datasetType, root, dataId):
        pathId = self._mapActualToPath(self._mapIdToActual(dataId))
        path = os.path.join(getattr(self, root),
                getattr(self, datasetType + 'Template') % pathId)
        return ButlerLocation(
                "lsst.afw.image.DecoratedImageU", "DecoratedImageU",
                "FitsStorage", path, dataId)

    def std_raw(self, item, dataId):
        md = item.getMetadata()
        md.set("LSSTAMP", "%(raft)s %(sensor)s %(channel)s" % dataId)
        newItem = afwImage.makeExposure(
                afwImage.makeMaskedImage(item.getImage()))
        newItem.setMetadata(md)
        return newItem

    def _mapIdToActual(self, dataId):
        # TODO map mapped fields in actualId to actual fields
        return dict(dataId)

    def _mapActualToPath(self, actualId):
        pathId = dict(actualId)
        if pathId.has_key("raft"):
            pathId['raft'] = re.sub(r'R:(\d),(\d)', r'R\1\2', pathId['raft'])
        if pathId.has_key("sensor"):
            pathId['sensor'] = re.sub(r'S:(\d),(\d)', r'S\1\2',
                    pathId['sensor'])
        if pathId.has_key("channel"):
            pathId['channel'] = re.sub(r'C:(\d),(\d)', r'C\1\2',
                    pathId['channel'])
        if pathId.has_key("detector"):
            for m in re.finditer(r'([RSC]):(\d),(\d)', pathId['detector']):
                id = m.groups(0) + m.groups(1) + m.groups(2)
                if m.groups(0) == 'R':
                    pathId['raft'] = id
                elif m.groups(0) == 'S':
                    pathId['sensor'] = id
                elif m.groups(0) == 'C':
                    pathId['channel'] = id
        if pathId.has_key("snap"):
            pathId['exposure'] = pathId['snap']
        return pathId

MinMapper.map_raw = \
        lambda self, dataId, write: self._pathMapper("raw", "root", dataId)

for calibType in ["bias", "dark", "flat", "fringe"]:
    setattr(MinMapper, "map_" + calibType, lambda self, dataId, write:
            self._pathMapper(calibType, "calibRoot", dataId))
