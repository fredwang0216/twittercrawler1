'''
Created on Jun 23, 2014

@author: christian
'''
import math


class Region:
    
    def __init__(self):
        self.geo_golygon = []
        self.max_lat = -90
        self.max_lon = -180
        self.min_lat = 90
        self.min_lon = 180
    
    
    def init_geo_polygon(self, file_name):
        self.geo_golygon = []
        
        try:
            coord_file = open(file_name, "r")
        except:
            raise Exception("Unable to open file " + file_name)
        
        try:
            while 1:
                line = coord_file.readline()
    
                if line.startswith('#'):
                    continue
                    
                if not line:
                    break
    
                line = line.replace("\r","")
                line = line.replace("\n","")
                line = line.replace(" ","")
                
                (lat, lon) = line.split(",")
                lat_float = float(lat)
                lon_float = float(lon)
                self.geo_golygon.append((lat_float, lon_float))
                
                if lat_float > self.max_lat:
                    self.max_lat = lat_float
                if lat_float < self.min_lat:
                    self.min_lat = lat_float
                if lon_float > self.max_lon:
                    self.max_lon = lon_float
                if lon_float < self.min_lon:
                    self.min_lon = lon_float
                
        except:
            raise Exception("Unable to initialize polygon (source file: " + file_name + ")")



    def coord_in_polygon(self, lat, lon):
        angle = 0
        n = len(self.geo_golygon)
        if n == 0:
            raise Exception("Geo polygon not initialized")
        
        for i in range(0, n):
            point1_lat = self.geo_golygon[i][0] - lat
            point1_long =self.geo_golygon[i][1] - lon
            point2_lat = self.geo_golygon[(i+1)%n][0] - lat
            point2_long = self.geo_golygon[(i+1)%n][1] - lon
            #print point1_lat, point1_long, point2_lat, point2_long
            angle += self.angle_2d(point1_lat,point1_long,point2_lat,point2_long);
        
        if (math.fabs(angle) < math.pi):
            return False
        else:
            return True
    
    
    def angle_2d(self, x1, y1, x2, y2):
        theta1 = math.atan2(y1,x1)
        theta2 = math.atan2(y2,x2)
        dtheta = theta2 - theta1;
        while dtheta > math.pi:
            dtheta -= 2*math.pi;
        while dtheta < -math.pi:
            dtheta += 2*math.pi;
        
        return dtheta
    
    
    
if __name__ == "__main__":
    
    region = Region()
    region.init_geo_polygon("/home/christian/work/development/eclipse-workspace/IncidenceReporter/polygon-singapore.txt")
    print region.coord_in_polygon(1.337377, 103.812507)
    