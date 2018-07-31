'''
Created on Apr 2, 2015

@author: BerkeKhan
'''
#!/usr/bin/env python

'''
Created on Feb 25, 2013

@author: ahmed
'''
#!/usr/bin/env python


import random
import numpy.random as np
import sys
import math,time
import csv
import datetime

import os
import threading
import numpy as nump

import wavelet
import HybridModel
import utility

class sim(object):

    def __init__(self,name,Server_Speed=22,D=15,Factor=1,R=0,delta_T=5,repair_c=0, Initial_Number_Servers=1):
        np.seed(0)
        self.name=name
        random.seed(1)
        self.LessRequests=[]
        self.LessServers=[]
        self.ExtraRequests=[]
        self.ExtraServers=[]
        self.LessRequestsMin=[]
        self.LessServersMin=[]
        self.ExtraRequestsMin=[]
        self.ExtraServersMin=[] 
        self.TotalServers=[]
        self.TotalServersMin=[]
        self.Server_Speed_Avg=Server_Speed
        self.AvgServer_Speed=Server_Speed
        self.Server_Speed=Server_Speed
        self.Factor=Factor
        self.D=D
        TraceFile=open("%s"%(name).strip(),'r')
        self.Trace=TraceFile.readlines()
        self.R=R
        self.delta_T=delta_T
        self.AvgCapacity=Initial_Number_Servers
        self.repair_c=repair_c            #estimates
        self.CurrentCapacity=Initial_Number_Servers
        self.Predicted=0
        self.Time_lastEstimation=0
#         self.outTime=open('./AGILEHrWikiResults/AGILEGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
#         self.outStat=open('./AGILEHrWikiResults/StatAGILEGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
#         self.outFuture=open('./AGILEHrWikiResults/FutureAGILEGen2%sSS%sD%sF%sT%s.txt'%(name.strip().split("/")[-1],Server_Speed,D,Factor,delta_T),'w')
        self.lastTime=0
        self.Second_Previous=0
        self.Minute_Previous=0
        self.Hour_Previous=0
        self.NumberOfReactiveServers=0
        self.delta_Hours=0
        self.delta_minutes=0
        self.delta_seconds=0
        self.sigma_Alive=1
        self.Current_Load=0
        self.avg_n=1
        self.u_estimate=0
        self.P_estimate=0
        self.Previous_Load=0
        self.s=0
        np.seed(0)
        self.NumberOfProactiveDecisions=0
        self.numberOfestimations=0
        self.numberReactiveDecisions=0
        self.PastMinute=0
        self.FutureMinute=0
        self.Current_Hour=0
        self.AGILE={}
        self.Buffer=0
        self.BufferedLoad=0
        self.Performance=[Server_Speed]
        self.JobsList=[]
        self.SumOptimal=0.0
        self.SumActual=0.0
        self.SumExtra=0.0
        self.SumLess=0.0
        self.SumOptimalQueue=0.0
        self.SumDeployed=0.0
        self.TimeAgg=0
        self.Load=[]
        self.Capacity=[]
        self.lookAhead=0.0
        self.Delta_Load=0
        with open("./SlowerAGILERevHrWikiResults/consiceResults%s.csv"%(name.strip().split("/")[-1]), "wb") as csv_file:    
            self.writer=csv.writer(csv_file,delimiter=',')
            self.writer.writerow(["Current_Time","CapRequiredServers", "CurrentCapacity","RunningJobs","Average Server Speed","Load_Requests","Buffer","Current_Load", "Past Minute", "FutureMinute","Calculation Time"])
        
        ##conf for moving prediction into Master:
        self.clone_triggering = "clonescale"
        self.prediction_step = 1
        self.training_size = 200 #samples
        #max_copyrate = 30 * 1024 * 1024 #MB/S
        self.pridiction_interval = 1 #seconds
        self.transfer_rate = "default"

    def csv_writer(self,data, path):
        """
        Write data to a CSV file path
        """
        with open(path, "wb") as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            for line in data:
                writer.writerow(line)    
                
    def run_sim(self):
        x=self.simloop()
        return x
                
            

    def makePrediction(self, input, step):
        fake_predictions = [[100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
                        ]
    
        #fake_predictions = [[0, 0, 0, 0, 0, 0, 0, 100, 100, 100], \
        #                    [0, 0, 0, 0, 0, 0, 100, 100, 100, 100], \
        #                    [0, 0, 0, 0, 0, 100, 100, 100, 100, 100], \
        #                    [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
        #                    [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
        #                    [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
        #                    [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
        #                    [100, 100, 100, 100, 100, 100, 100, 100, 100, 100], \
        #                    ]
        predictions = None
        
        
        #PRESS
        if self.clone_triggering == "press":
            predictions = HybridModel.predictHybrid(input,step)
        else:
        #wavelet
            predictions = HybridModel.predictSignature(input, step)
            if predictions != None:
               print("predictor got signature")
            else:
               print("predictor didn't get signature")
               if step <= 40:
                    predictions = wavelet.wavelet_prediction(input, 40, 'haar', dynamic_wavelet = True)[:step]
               else:
                    predictions = wavelet.wavelet_prediction(input, step, 'haar', dynamic_wavelet = True)            
        return predictions
     
    def LoadCalculator(self):
        try:
            RunningJobsTimes=len(self.JobsList)
            if RunningJobsTimes==60:
                self.JobsList.pop(0)
                self.JobsList[-2]-=math.ceil(self.JobsList[-2]*0.2)
                self.JobsList[-3]-=math.ceil(self.JobsList[-3]*0.15)
                self.JobsList[-4]-=math.ceil(self.JobsList[-4]*0.207)
                self.JobsList[-7]-=math.ceil(self.JobsList[-6]*0.18)
                self.JobsList[-12]-=math.ceil(self.JobsList[-12]*0.19)
                self.JobsList[-21]-=math.ceil(self.JobsList[-21]*0.23)
                self.JobsList[-31]-=math.ceil(self.JobsList[-31]*0.29)
            elif  RunningJobsTimes>0:
                self.JobsList[-2]-=math.ceil(self.JobsList[-2]*0.2)
                self.JobsList[-3]-=math.ceil(self.JobsList[-3]*0.15)
                self.JobsList[-4]-=math.ceil(self.JobsList[-4]*0.207)
                self.JobsList[-7]-=math.ceil(self.JobsList[-6]*0.18)
                self.JobsList[-12]-=math.ceil(self.JobsList[-12]*0.19)
                self.JobsList[-21]-=math.ceil(self.JobsList[-21]*0.23)
                self.JobsList[-31]-=math.ceil(self.JobsList[-31]*0.29)
        except:
            pass
        
    def simloop(self):
        GammaTime=0
        self.PreviousCapacity=1
        Time=0
        self.Previous_Load=0
        self.Time_Previous=float(self.Trace[0].split()[1])
        initial_Time=self.Time_Previous
        self.PastMinute=float(self.Trace[0].split()[2])*self.Factor
        self.delta_Time=1
        GammaTime=self.Time_Previous
        self.PastMinute=1#math.ceil(self.PastMinute/self.Server_Speed)*3
        Delta_Load=0
        CapacityList=[1]  
        LoadServers=[self.PastMinute]
        self.Predicted=math.ceil(self.PastMinute/self.AvgServer_Speed)
#         print>>self.outTime, self.Predicted
        for t in self.Trace:
            #print self.JobsList
            self.Server_Speed=np.poisson(self.Server_Speed_Avg) #std deviation for 1000 requests =50
            t=t.split()
            Current_Time=self.Time_Previous+1#float(t[1])
            if self.Predicted>0:
                self.CurrentCapacity=float(self.Predicted)/self.Server_Speed
            else:
                self.CurrentCapacity=float(self.Current_Load)/self.Server_Speed
            self.Current_Load=float(t[2])
#            self.LoadCalculator()
#            self.JobsList+=
 #           if self.CurrentCapacity*self.Server_Speed>= sum(self.JobsList)+self.Current_Load+self.Buffer:
  #              self.JobsList[-1]=self.Current_Load+self.Buffer
   #             self.Buffer=0
    #        else:
     #           self.Buffer+=(sum(self.JobsList)+self.Current_Load-(self.CurrentCapacity*self.Server_Speed))
      #          if self.CurrentCapacity*self.Server_Speed-sum(self.JobsList)>0:
       #             self.JobsList[-1]=self.CurrentCapacity*self.Server_Speed-sum(self.JobsList)
            Load=int(math.ceil(float(self.Current_Load)))
            
            LoadServers+=[int(math.ceil(float(self.Current_Load)))]
#             print Load
            #print Load
            self.delta_Time=1
            self.Time_Previous=Current_Time          
            x=Current_Time-self.Time_lastEstimation           
            tempCapac=0
            t1=time.time()
            if len(LoadServers)<6000:
                self.Predicted=math.ceil(Load)
            else:
                if Load>=self.Server_Speed:
                    self.Predicted=self.makePrediction(LoadServers, self.prediction_step)[0]
                else:
                    self.Predicted=self.Server_Speed/3
                LoadServers.pop(0)
            
                
                
            
            
            t2=time.time()
#             print len(LoadServers)
#             print>>self.outTime, t2-t1
#             self.Load+=[LoadServers]
#             self.Capacity+=[self.CurrentCapacity]
#             self.TimeAgg+=1
#             self.SumOptimal+=self.Current_Load/self.Server_Speed
#             self.SumOptimalQueue+=LoadServers
#             self.SumActual+=self.CurrentCapacity
#             self.numberOfestimations+=abs(self.PastMinute)            
            with open("./SlowerAGILERevHrWikiResults/consiceResults%s.csv"%(self.name.strip().split("/")[-1]), "a") as csv_file:
                    self.writer=csv.writer(csv_file,delimiter=',')    
                    self.writer.writerow([t[0],int(math.ceil(float(Load)/self.Server_Speed)),int(math.ceil(self.CurrentCapacity)),int(sum(self.JobsList)),self.Server_Speed,self.Current_Load,self.Buffer,self.Current_Load, self.PastMinute,self.FutureMinute, t2-t1])            
#         for i in range(len(self.Capacity)):
#             try:
#                 FutureLoad=self.Load[i:i+10]
#             except:
#                 FutureLoad=self.Load[i:]
#             MAX=max(FutureLoad)
#             if self.Capacity[i]-MAX>0: 
#                 self.SumExtra+=self.Capacity[i]-MAX
#                 self.lookAhead+=FutureLoad.index(MAX)
#                 print>>self.outFuture, MAX, self.SumExtra, self.SumLess
#             elif self.Capacity[i]-self.Load[i]<0:
#                 self.SumLess+=self.Capacity[i]-self.Load[i]
#                 print>>self.outFuture, "Less",  MAX, self.SumExtra, self.SumLess
#             else:
#                 pass
#         print>>self.outStat,'SumOptimal',self.SumOptimal,'AvgOptimal',self.SumOptimal/self.TimeAgg,
#         print >>self.outStat,'SumOptimalQueue',self.SumOptimalQueue,'AvgOptimalQueue',self.SumOptimalQueue/self.TimeAgg,'SumExtra',
#         print>>self.outStat, self.SumExtra,'AvgExtra',self.SumExtra/self.TimeAgg,'SumLess',self.SumLess,'AvgLess',self.SumLess/self.TimeAgg, 'ActualAvg',
#         print>>self.outStat, self.SumActual/self.TimeAgg, " Horizon ",self.lookAhead/self.TimeAgg, "NumberOfChanges",self.numberOfestimations,"AvgNumberOfChanges",self.numberOfestimations/self.TimeAgg
#                             
        
