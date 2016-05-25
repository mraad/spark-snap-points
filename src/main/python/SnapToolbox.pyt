#
# http://www.lfd.uci.edu/~gohlke/pythonlibs/#python-snappy (python_snappy-0.5-cp27-none-win32.whl)
# pip install fastavro
# pip install hdfs
#

import arcpy
import fastavro as avro
from hdfs import InsecureClient


class Toolbox(object):
    def __init__(self):
        self.label = "Toolbox"
        self.alias = "Toolbox"
        self.tools = [HDFSTool]


class HDFSTool(object):
    def __init__(self):
        self.label = "Import Snaps"
        self.description = "Import snap data from HDFS in Avro or WKT format"
        self.canRunInBackground = True

    def getParameterInfo(self):
        feat_layer = arcpy.Parameter(
            name="out_fc",
            displayName="Snaps",
            direction="Output",
            datatype="Feature Layer",
            parameterType="Derived")

        host = arcpy.Parameter(
            name="in_host",
            displayName="Host",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        host.value = "quickstart"

        user = arcpy.Parameter(
            name="in_user",
            displayName="User",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        user.value = "root"

        path = arcpy.Parameter(
            name="in_path",
            displayName="Path",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        path.value = "/user/root/avro"

        data_format = arcpy.Parameter(
            name="in_format",
            displayName="Data Format",
            direction="Input",
            datatype="GPString",
            parameterType="Required")
        data_format.value = "avro"
        data_format.filter.type = "ValueList"
        data_format.filter.list = ["wkt", "avro"]

        return [feat_layer, host, user, path, data_format]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        host = parameters[1].value
        user = parameters[2].value
        path = parameters[3].value
        is_wkt = parameters[4].value == "wkt"

        sp_ref = arcpy.SpatialReference(4269)
        fc = "in_memory/Snaps"
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)
        arcpy.management.CreateFeatureclass("in_memory", "Snaps", "POLYLINE",
                                            spatial_reference=sp_ref,
                                            has_m="DISABLED",
                                            has_z="DISABLED")
        arcpy.management.AddField(fc, "RHO", "FLOAT")
        arcpy.management.AddField(fc, "SIDE", "TEXT")
        arcpy.management.AddField(fc, "PID", "LONG")
        arcpy.management.AddField(fc, "LID", "LONG")

        fields = ['SHAPE@WKT'] if is_wkt else ['SHAPE@']
        fields.extend(['RHO', 'SIDE', 'PID', 'LID'])
        with arcpy.da.InsertCursor(fc, fields) as cursor:
            client = InsecureClient('http://{}:50070'.format(host), user=user)
            parts = client.parts(path)
            arcpy.SetProgressor("step", "Importing...", 0, len(parts), 1)
            for part in parts:
                arcpy.SetProgressorLabel("Importing {0}...".format(part))
                arcpy.SetProgressorPosition()
                if is_wkt:
                    with client.read("{}/{}".format(path, part), encoding='utf-8', delimiter='\n') as reader:
                        for line in reader:
                            t = line.split('\t')
                            if len(t) == 5:
                                cursor.insertRow([t[0], float(t[1]), t[2], int(t[3]), int(t[4])])
                else:
                    with client.read("{}/{}".format(path, part)) as reader:
                        for row in avro.reader(reader):
                            px = row['px']
                            py = row['py']
                            qx = row['x']
                            qy = row['y']
                            rho = row['rho']
                            side = row['side']
                            if side == 0:
                                side_text = 'O'
                            elif side == 1:
                                side_text = 'R'
                            else:
                                side_text = 'L'
                            attr = row['attr']
                            shape = [[px, py], [qx, qy]]
                            cursor.insertRow((shape, rho, side_text, int(attr[0]), int(attr[1])))
            arcpy.ResetProgressor()
        parameters[0].value = fc
