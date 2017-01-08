#!/usr/bin/python
#coding:utf-8
import numpy as np
import pandas as pd
from datetime import datetime
import os

class BusLineHandler:
	def __init__(self,path):
		self.raw_data = pd.read_excel(path)
	def parseBusNumber(self,busName):
		end = busName.find('(')
		if end != -1:
			return busName[:end]
	def splitWithCom(self,path):
		out = open(path,'w')
		lines=[]
		record_len = self.raw_data.shape[0]
		count = 0
		lastBusNumber = ''
		for i in range(record_len):
			city = self.raw_data.iloc[i][0]
			if city == u'北京':
				busNumber = self.parseBusNumber(self.raw_data.iloc[i][1])
				if lastBusNumber != busNumber:
					lastBusNumber = busNumber
					direct = 0
				else:
					direct = 1
				line = city+','+busNumber+','+str(direct)+','+self.raw_data.iloc[i][2]+'\n'
				lines.append(line.encode('utf-8'))
			i+=1
		out.writelines(lines)

if __name__ == '__main__':
	path = 'E:\BusPredict\\raw_data\\busline.xlsx'
	outpath = 'E:\BusPredict\\raw_data\\busline'
	blh=BusLineHandler(path)
	blh.splitWithCom(outpath)
